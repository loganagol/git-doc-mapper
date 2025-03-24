/**
 * @fileoverview
 * This file is triggered by WEBSERVICE and serves as an API endpoint for the gitaim interface.
 * 
 * @global @type {javax.servlet.http.HttpServletRequestWrapper} request
 * @global @type {javax.servlet.http.HttpServletResponseWrapper} response
 */

const MODULE_PATH = standardModulePath;
const CUSTOM_MODULE_PATH = customModulePath;
bootstrapRequire();
const actionCodeConfiguration = require(MODULE_PATH + "action-code-configuration_1.4");
const logMessage = require(MODULE_PATH + "log-message_1.0");

const Facade = Java.type("com.maximus.fmax.Facade");
const SystemContext = Java.type("com.maximus.fmax.common.framework.util.impl.SystemContext");
const DTOFactory = Java.type("com.maximus.fmax.common.framework.dto.DTOFactory");
const AeDocumentVersionSeriesPK = Java.type("com.maximus.fmax.cms.dto.AeDocumentVersionSeriesPK");
const MultiPartParser = Java.type("com.maximus.fmax.mdesk.multipart.MultiPartParser");
const UploadFilePart = Java.type("com.maximus.fmax.mdesk.multipart.UploadFilePart");
const UploadParameterPart = Java.type("com.maximus.fmax.mdesk.multipart.UploadParameterPart");
const LinkedList = Java.type("java.util.LinkedList");
const Files = Java.type("java.nio.file.Files");
const FileOutputStream = Java.type("java.io.FileOutputStream");
const ByteBuffer = Java.type("java.nio.ByteBuffer");
const BufferedReader = Java.type("java.io.BufferedReader");
const InputStreamReader = Java.type("java.io.InputStreamReader");
const DateTimeFormatter = Java.type("java.time.format.DateTimeFormatter");

//// SCRIPT START //////////////////////////////////////////////////////////////////////////////////
const configInstance = actionCodeConfiguration.createInstance(actionCodeDocument);
const config = {
	"ACTION_CODE_TITLE": "git-doc-mapper",
	"DEBUG": configInstance.get("DEBUG", true)
};

logMessage.init(config.ACTION_CODE_TITLE, config.DEBUG);
const log = logMessage.getInstance();

const dtoFactory = DTOFactory.getInstance();
const systemContext = SystemContext.getInstance();
const cmsFacade = Facade.CMS.getFacade(systemContext);

class Router {
	constructor() {
		this.routes = {};
	}

	registerNewRoute(method, route, handler) {
		if (!this.routes[method]) {
			this.routes[method] = {};
		}
		this.routes[method][route] = handler;
	}

	handleRoute(method, route, ...args) {
		logDebug(`${this.constructor.name} handling: Method [${method}] route [${route}]`);

		if (this.routes[method] && this.routes[method][route]) {
			let expectedParams = this.routes[method][route].length;
			let providedParams = args.length;

			if (expectedParams == providedParams) {

				return this.routes[method][route].apply(this, args);
			} else {
				let errorMsg = `Function signature mismatch for [${method}] [${route}]; got [${providedParams}] expected [${expectedParams}]`;
				logError(errorMsg);
			}
		} else {
			return this.routeNotFound(method, route, args);
		}
	}

	routeNotFound(method, route) {
		let errorMsg = `No route found; unable to process request for method [${method}] route [${route}]`;
		logError(errorMsg);
	}
}

class RequestRouter extends Router {
	constructor() {
		super();
		this.registerNewRoute("POST", 'push', this.routePush);
		this.registerNewRoute("POST", 'pull', this.routePull);
	}

	/**
	 * @returns {{
	 * 		string: {
	 * 			"_doc_ver_id": string,
	 * 			"_version_label": string,
	 * 			"_edit_date": string
	 * 		}
	 * }} An object where each key is a doc id, and each value is an object containing doc version id, version label, edit date.
	 */
	routePush() {
		let { tempFiles, clientData } = this._parseMultiParts();

		let versionType = this._getVersionType(clientData["version_type"]);
		delete clientData["version_type"];
		let checkedInComment = JSON.stringify(clientData);

		let uploadResults = {};

		for (const [docId, filename, tempFile] of tempFiles) {
			let versionDTO = this._checkInNewDocument(docId, filename, tempFile, checkedInComment, versionType);

			if (versionDTO == null) {
				logError(`Document profile was not returned for [${docId}], check logs.`);
				continue;
			}

			let versionLabel = versionDTO.getVersionLabel();
			let docVerId = versionDTO.getDocVerId();
			logDebug(`Saved new document: docId [${docId}] docVerId [${docVerId}] versionLabel [${versionLabel}]`);

			let localDateTime = versionDTO.getEditDate().toLocalDateTime();
			let editDateString = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(localDateTime);
			
			// we could use filename as key here to avoid remapping on client, but docId is unique and filename isn't
			// plus the server doesn't seem to parse the original filepath, so we'll stick with the unique key
			uploadResults[docId] = { 
				"_doc_ver_id": docVerId,
				"_version_label": versionLabel,
				"_edit_date": editDateString
			}
		}
		return uploadResults;
	}

	routePull() {
		// pass
	}

	/**
	 * @returns {{
	 * 		"tempFiles": [[string, string, java.io.File]],
	 * 		"clientData": {string: string}
	 * }} 
	 * `clientData`: An object containing instructions and data from the client instance.
	 * `tempFiles`: An array of arrays, where each sub-array contains the `docId`, `filename`, 
	 * and a temporary file object.
	 */
	_parseMultiParts() {
		let tempFiles = [];
		let clientData = {};
		
		let parser = new MultiPartParser(request, true, "UTF-8");
		let part = parser.readNextPart();

		while (part != null) {
			if (part instanceof UploadFilePart) {
				logDebug(`UploadFilePart: name [${part.getName()}] contentType [${part.getContentType()}] filename [${part.getFileName()}] filepath [${part.getFilePath()}]`);
				
				/** !!! we have to write the temp file now, or the file length will be 0 later !!! */
				let tempFile = this._writeTempFile(part);
				tempFiles.push([part.getName(), part.getFileName(), tempFile]);
			} else if (part instanceof UploadParameterPart) {
				logDebug(`UploadParameterPart: stringValue [${part.getStringValue()}]`);

				try {
					clientData = JSON.parse(part.getStringValue());
				} catch (e) {
					if (e instanceof SyntaxError); // pass
					else throw e;
				}
			}
			part = parser.readNextPart();
		}
		return { tempFiles, clientData };
	}

	/**
	 * @param {com.maximus.fmax.mdesk.multipart.UploadFilePart} part - request file multipart
	 * @returns {java.io.File} file to write to repo
	 */
	_writeTempFile(part) {
		let tempFilePath = Files.createTempFile(null, null); // goes in C:\aim134\temp\		
		let tempFile = tempFilePath.toFile();
		let fos = new FileOutputStream(tempFile);

		try {
			part.writeTo(fos);
		} catch (e) {
			logError(e);
		} finally {
			fos.close();
		}
		logDebug(`Created temp file path ${tempFilePath.toString()} length ${tempFile.length()}`);
		return tempFile;
	}

	/**
	 * @param {string} docId - document version series (AeDocumentVersionSeriesDTO) doc id
	 * @param {string} filename - added to document version
	 * @param {java.io.File} tempFile - file to save
	 * @param {string} checkedInComment - added to document version; holds our gitaim client data
	 * @param {string} versionType - [M]ajor | Mi[P]nor... idk
	 * @returns {com.maximus.fmax.cms.dto.AeDocumentVersionDTO|null} current document version DTO; null if exception
	 */
	_checkInNewDocument(docId, filename, tempFile, checkedInComment, versionType) {
		let versionSeriesDTO = null;
		let versionDTO = null;
		try {
			versionSeriesDTO = cmsFacade.findByPrimaryKey(new AeDocumentVersionSeriesPK(docId), true);
			cmsFacade.checkout(versionSeriesDTO);
			versionSeriesDTO = cmsFacade.findByPrimaryKey(new AeDocumentVersionSeriesPK(docId), true);

			let versionDTO = cmsFacade.templateAeDocumentVersion(versionSeriesDTO);
			versionDTO.setCheckedInComment(checkedInComment);
			versionDTO.setFilename(filename);
			versionDTO.setMimeType(20); // FIXME: JS only for now
			
			versionSeriesDTO.addAeDocumentVersionDTO(versionDTO);
			cmsFacade.save(versionSeriesDTO, tempFile, versionType, true);
			logDebug(`Saved new document version in document version series [${docId}]`);

			versionSeriesDTO = cmsFacade.findByPrimaryKey(new AeDocumentVersionSeriesPK(docId), true);
			versionDTO = cmsFacade.getCurrentVersion(versionSeriesDTO, true); // no idea what boolean `includePwc` is
			logDebug(`Retrieved current document version [${versionDTO.getDocVerId()}]`);

			return versionDTO;
		} catch (e) {
			logError(e);
			// this._cancelCheckout(versionSeriesDTO);
		} finally {
			tempFile.delete();
		}
		return versionDTO;
	}

	_getVersionType(versionLabel) {
		const MAJOR_VERSION_FLAG = "M";
		const MINOR_VERSION_FLAG = "P";

		if (versionLabel.toUpperCase() == "MAJOR") {
			return MAJOR_VERSION_FLAG;
		} else if (versionLabel.toUpperCase() == "MINOR") {
			return MINOR_VERSION_FLAG;
		} else {
			throw new Error(`UNRECOGNIZED VERSION LABEL: ${versionLabel}`);
		}
	}

	_cancelCheckout(aeDocumentVersionSeriesDTO) {
		if (aeDocumentVersionSeriesDTO != null && aeDocumentVersionSeriesDTO.getDocState() == 1) {
			cmsFacade.cancelCheckout(aeDocumentVersionSeriesDTO);
			cmsFacade.save(aeDocumentVersionSeriesDTO, false);
		}
	}
}

class ResponseRouter extends Router {
	constructor() {
		super();
		this.registerNewRoute("POST", 'push', this.routePush);
		this.registerNewRoute("POST", 'pull', this.routePull);
		this.registerNewRoute("POST", 'show', this.routeShow);
	}

	/**
	 * @param {{
	 * 		string: {
	 * 			"_doc_ver_id": string,
	 * 			"_version_label": string,
	 * 			"_edit_date": string
	 * 		}
	 * }} results - an object containing all updated version information,
	 * where the keys are the document version series' `docId`
	 */
	routePush(results) {
		let resultsString = JSON.stringify(results);
		this._print(resultsString);
	}

	routePull() {
		// pass
	}

	_print(content) {
		let writer = response.getWriter();
		writer.print(content);

		response.setContentType("application/json");
	}
}

logInfo("SCRIPT STARTED");
runScript();
logInfo("SCRIPT ENDED");

////////////////////////////////////////////////////////////////////////////////////////////////////
function runScript() {
	let method = request.getMethod();
	let route = request.getParameter("route");

	let requestRouter = new RequestRouter();
	let responseRouter = new ResponseRouter();

	let reqResults = requestRouter.handleRoute(method, route);
	let resResults = responseRouter.handleRoute(method, route, reqResults);
}

function DEBUG_inputStreamToTempFile(inputStream) {
	let tempFilePath = Files.createTempFile(null, null);
	let tempFile = tempFilePath.toFile();
	tempFile.deleteOnExit();

	let fos = new FileOutputStream(tempFile);
	try {
		let buffer = ByteBuffer.allocate(1024);
		let bytesRead = 0;
		let byteArray = Java.to(new Array(1024), 'byte[]');

		while ((bytesRead = inputStream.read(byteArray)) !== -1) {
			buffer.clear();
			buffer.put(byteArray, 0, bytesRead);
			buffer.flip();

			while (buffer.hasRemaining()) {
				fos.write(byteArray, 0, bytesRead);
			}
		}
	} finally {
		fos.close();
		inputStream.close()
	}

	return tempFile;
}

function DEBUG_inputStreamToString(inputStream) {
	try {
		let reader = new BufferedReader(new InputStreamReader(inputStream));
		let body = [];
		let line;

		while ((line = reader.readLine()) !== null) {
			body.push(line);
		}
		reader.close();
		return body.join("\n");
	} catch (e) {
		logError(e);
	}
	return null;
}

function logDebug(msg) {
	if (config.DEBUG) log.logMessage(msg);
}

function logInfo(msg) {
	log.logMessage(msg);
}

function logError(msg) {
	log.logErrorMessage(msg);
}

function bootstrapRequire() {
	if (typeof require != "function" && typeof load == "function") {
		load(MODULE_PATH + "jvm-npm_1.0.js");
	}
}