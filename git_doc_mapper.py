#!/usr/bin/env python3

import argparse
import configparser
import json
import logging
import os
import requests
import shutil
import subprocess
import sys

from abc import ABC, abstractmethod
from argparse import ArgumentParser
from bs4 import BeautifulSoup
from datetime import datetime
from enum import Enum
from getpass import getpass
from pathlib import Path
from typing import List, Dict, Any, Union, Tuple, TextIO
from urllib.parse import urlparse, urljoin

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
MAP_FILENAME = config['general']['map_filename']

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    # format='%(asctime)s - %(levelname)s - %(message)s',
    format='%(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler() # console/stdout
    ]
)

class SortDirectionEnum(Enum):
    """
    From `com.maximus.fmax.common.framework.dto.SortDirection`
    """
    ASCENDING = "ASCENDING"
    DESCENDING = "DESCENDING"
    UNSPECIFIED = "UNSPECIFIED"

class SQLOperatorEnum(Enum):
    """
    From `com.maximus.fmax.common.framework.dao.util.SQLOperator`
    """
    BETWEEN = "BETWEEN"
    CONTAINS = "CONTAINS"
    CURRENT_USER = "CURRENT_USER"
    CURRENT_USER_EMPLOYEE = "CURRENT_USER_EMPLOYEE"
    CURRENT_USER_SHOP = "CURRENT_USER_SHOP"
    EMPTY = "EMPTY"
    ENDS_WITH = "ENDS_WITH"
    EQUAL = "EQUAL"
    EQUAL_COLUMN = "EQUAL_COLUMN"
    EQUAL_OR_NULL = "EQUAL_OR_NULL"
    EXISTS = "EXISTS"
    GREATERTHAN = "GREATERTHAN"
    GREATERTHAN_COLUMN = "GREATERTHAN_COLUMN"
    GREATERTHANOREQUAL = "GREATERTHANOREQUAL"
    GREATERTHANOREQUAL_COLUMN = "GREATERTHANOREQUAL_COLUMN"
    GREATERTHANPERCENT_COLUMN = "GREATERTHANPERCENT_COLUMN"
    IN = "IN"
    INLAST = "INLAST"
    INNEXT = "INNEXT"
    LESSTHAN = "LESSTHAN"
    LESSTHAN_COLUMN = "LESSTHAN_COLUMN"
    LESSTHANOREQUAL = "LESSTHANOREQUAL"
    LESSTHANOREQUAL_COLUMN = "LESSTHANOREQUAL_COLUMN"
    LESSTHANPERCENT_COLUMN = "LESSTHANPERCENT_COLUMN"
    LIKE = "LIKE"
    MATCH_ALL = "MATCH_ALL"
    MATCH_ANY = "MATCH_ANY"
    NEWERTHAN = "NEWERTHAN"
    NOT_CONTAINS = "NOT_CONTAINS"
    NOT_EMPTY = "NOT_EMPTY"
    NOT_EXISTS = "NOT_EXISTS"
    NOT_EXPIRED = "NOT_EXPIRED"
    NOT_IN = "NOT_IN"
    NOT_LIKE = "NOT_LIKE"
    NOT_NULL = "NOT_NULL"
    NOTEQUAL = "NOTEQUAL"
    NULL = "NULL"
    OLDERTHAN = "OLDERTHAN"
    REALLY_EQUAL = "REALLY_EQUAL"
    REALLY_GREATERTHANOREQUAL = "REALLY_GREATERTHANOREQUAL"
    REALLY_LESSTHANOREQUAL = "REALLY_LESSTHANOREQUAL"
    STARTS_WITH = "STARTS_WITH"
    WITHIN = "WITHIN"

class ColumnSpecification:
    """
    Define a field returned by a FindList query.
    Primary keys in the DTO will be returned by default without specifying.
    DTO column name must be camelCase version of table column name.
    """
    def __init__(self, property:str, direction:SortDirectionEnum=SortDirectionEnum.UNSPECIFIED):
        self.property = property
        self.direction = direction
    
    def to_dict(self):
        return {
            'property': self.property,
            'direction': self.direction.value
        }
    
class ColumnSpecificationListBuilder:
    """
    Builds a list of columns to return.
    """
    def __init__(self):
        self.column_specifications = []

    def add_column_spec(self, columnSpec: ColumnSpecification):
        self.column_specifications.append(columnSpec)
    
    def to_list(self):
        return [col.to_dict() for col in self.column_specifications]

class AttributeSQL:
    """
    Define a DTO attribute to search by using FindList query.
    DTO attribute name must be camelCase version of table column name.
    """
    def __init__(self, colName:str, values:List, sql_operator:SQLOperatorEnum):
        self.colName = colName
        self.values = values
        self.sql_operator = sql_operator

    def to_dict(self):
        return {
            self.colName: {
                "values": self.values,
                "sqlOperator": self.sql_operator.value
            }
        }

class AttributeEquals:
    def __init__(self, colName:str, value: str):
        self.colName = colName
        self.value = value
    
    def to_dict(self):
        return {
            self.colName: self.value
        }

class AttributeListBuilder:
    """
    Builds a list of attributes to query by.
    """
    def __init__(self):
        self.attributes = []

    def add_attribute(self, attribute: Union[AttributeSQL, AttributeEquals]):
        self.attributes.append(attribute)

    def to_dict(self):
        return {
            "attributes": [attr.to_dict() for attr in self.attributes]
        }

class FindListQueryBuilder:
    """
    Builds a request body to use in a FindList query to the `PUT /crud/dto/list/<DTOName>` endpoint.

    Example usage:
        query = FindListQueryBuilder()
        query.add_column_spec(ColumnSpecification("statusCode", SortDirection.ASCENDING))
        query.add_column_spec(ColumnSpecification("utilityType"))
        query.add_attribute(AttributeSQL("statusCode", ["OPEN", "OPEN-VER-SUCCESS"], SQLOperator.IN))
        query.add_attribute(AttributeEquals("billingPeriod", "FY25-05-NOV"))
        query_body = query.to_dict()
    """
    
    def __init__(self, start:int=0, batch_size:int=500):
        self.start = start
        self.batch_size = batch_size
        self.column_specifications = ColumnSpecificationListBuilder()
        self.query = AttributeListBuilder()

    def add_column_spec(self, column_specification: ColumnSpecification):
        self.column_specifications.add_column_spec(column_specification)

    def add_attribute(self, attribute: Union[AttributeSQL, AttributeEquals]):
        self.query.add_attribute(attribute)

    def to_dict(self):
        return {
            "start": self.start,
            "batchSize": self.batch_size,
            "columnSpecifications": self.column_specifications.to_list(),
            "query": self.query.to_dict()
        }

    def to_json(self):
        return json.dumps(self.to_dict(), indent=4)

class APIAdaptor:
    DETAILS = {'details': 'true'}

    def __init__(self, url, webservice_id, username, password):
        self.url = self._validate_url(url)
        self.webservice_id = webservice_id
        self.auth = (username, password)

        log.info(f'Initialized API connector with URL [{url}]')

    def post_files(self, route: str, files: Dict):
        params = {
            'tranxNum': self.webservice_id,
            'route': route
        } 
        endpoint = urljoin(self.url, 'actioncode')
        log.debug(f'endpoint: {endpoint}')
        response = requests.post(url=endpoint, auth=self.auth, params=params, files=files)

        return self._response_hander(response)
    
    def find_list(self, dto_name:str, query_body:Dict):
        """Returns dictionary with 500 results maximum."""
        endpoint = urljoin(self.url, f'crud/dto/list/{dto_name}')
        log.debug(self.url)
        log.debug(endpoint)
        response = requests.put(url=endpoint, json=query_body, auth=self.auth)

        return self._response_hander(response)
    
    def find_hierarchy(self, dto_name:str, params:Dict, cascade: bool):
        endpoint = urljoin(self.url, f'crud/dto/{dto_name}')
        params = {**self.DETAILS, **params} if cascade else params
        response = requests.get(url=endpoint, params=params, auth=self.auth)

        return self._response_hander(response)
    
    def _parse_contents(self, response: requests.Response) -> Union[str, Dict, None]:
        """
        Attempts to parse out the body of incoming HTML responses based on Content-Type header.
        Parses HTML into just the body text, JSON into dictionary, and '' into None
        """
        content_type = response.headers.get('Content-Type', '').lower()
        contents = response.text if response.text else None

        if 'text/html' in content_type or 'application/xhtml+xml' in content_type:
            soup = BeautifulSoup(response.text, 'html.parser')
            body = soup.find('body')
            if body:
                contents = body.get_text(separator='\n', strip=True)

        elif 'application/json' in content_type:
            try:
                contents = response.json()
            except requests.exceptions.JSONDecodeError:
                log.error(f'Error while parsing JSON from response with Content-Type: {content_type}')

        return contents
    
    def _response_hander(self, response: requests.Response) -> Dict:
        # log.debug(f'Response headers: {response.headers}')
        contents = self._parse_contents(response)

        if response.status_code >= 200 and response.status_code <= 299:
            return contents
        elif response.status_code == 400:
            raise requests.exceptions.HTTPError(f'Bad request: HTTP Error 400: Server error message: {contents}')
        elif response.status_code == 401:
            raise requests.exceptions.HTTPError(f'Invalid auth: HTTP Error 401: Error connecting to server: {contents}')
        else:
            raise requests.exceptions.RequestException(f'Response error: Status code {response.status_code}: {contents}')
        
    def _validate_url(self, url: str):
        parsed_url = urlparse(url)
        path_parts = parsed_url.path.strip('/').split('/')

        if not parsed_url.netloc:
            raise ValueError(f'URL domain is invalid: {parsed_url.netloc}; {url}')
        if parsed_url.scheme != 'https':
            raise ValueError(f'URL scheme is not `https`; {url}')
        if not path_parts or 'fmax' not in path_parts[0]:
            raise ValueError(f'URL path is empty or does not contain `fmax`: {path_parts}; {url}')
        
        return url.rstrip('/') + '/' # remove and restore trailing / to make sure base url is in the right format for urljoin

class UserInputHandler:
    def __init__(self):
        pass

    @staticmethod
    def get_username_if_none(arg_username):
        username = arg_username if arg_username else input('Enter your username: ')
        return username
    
    @staticmethod
    def get_password_if_none(arg_password):
        password = arg_password if arg_password else getpass('Enter your password: ')
        return password
    
    @staticmethod
    def continue_Yn(prompt:str=None):
        return UserInputHandler._continue('Y', prompt)
    
    @staticmethod
    def continue_yN(prompt:str=None):
        return UserInputHandler._continue('N', prompt)
    
    @staticmethod
    def _continue(yn:str, prompt:str=None):
        yn = yn.upper()
        y = 'Y' if yn == 'Y' else 'y'
        n = 'N' if yn == 'N' else 'n'
        prompt = f'{prompt + " " if prompt else ""}Continue? ({y}/{n}): '

        choice = input(prompt).strip().upper()
        return choice != 'N' if yn == 'Y' else choice == 'Y'

class FileMap:
    def __init__(self, filename):
        self.tldir: str = run_cli_command(['git', 'rev-parse', '--show-toplevel'])
        self.filemap: Dict[str, Dict[str, Dict[str, str]]] = self._load_filemap(filename)

    def map_has_all_targets(self, api_connections: Dict[str, APIAdaptor]):
        api_set = set(api_connections.keys())
        map_set = set(self.filemap.get('_targets').keys())

        if not api_set.issubset(map_set):
            log.error(f'File map is missing servers: {api_set - map_set}')
            return False
        else:
            return True

    def get_mapped_files(self, document_profiles: Dict[str, str]) -> Dict[str, Tuple[str, TextIO, str]]: # List[Tuple[str, Tuple[str, TextIO]]]:
        files = {}

        for filename, doc_id in document_profiles.items():
            filepath = os.path.join(self.tldir, filename)
            try:
                with open(filepath, 'r') as file:
                    file_contents = file.read()
                    files[doc_id] = (filename, file_contents, 'text/plain') # 'application/javascript' is not recognized
                log.info(f'Added file: {filepath}')
            except FileNotFoundError as e:
                log.error(f'File was not found: {e}')
        
        return files

    def get_document_profiles(self, target: str) -> Dict[str, str]:
        target_properties = self.filemap.get('_targets').get(target)
        document_profiles = target_properties.get('_document_profiles')

        return document_profiles
    
    def get_module_directory(self, target: str) -> str:
        target_properties = self.filemap.get('_targets').get(target)
        module_directory = target_properties.get('_module_directory')

        return module_directory

    def _load_filemap(self, filename: str) -> Dict:
        filepath = Path(self.tldir, filename)

        if filepath.exists() and filepath.is_file():
            with open(filepath) as file:
                filemap = json.load(file)
                log.debug(f'Loaded file map at {filepath}')

                self._validate_map_files(filemap)

                return filemap
        else:
            self._create_filemap(filepath)
        
    def _validate_map_files(self, filemap: Dict[str, Dict[str, Dict[str, Any]]]):
        targets = filemap.get('_targets', {})
        for target, map in targets.items():
            # validate document profiles
            document_profiles = map.get('_document_profiles')
            for filename in document_profiles.keys():
                filepath = Path(self.tldir, filename)
                if not filepath.exists() or not filepath.is_file():
                    raise FileNotFoundError(f'File {filename} is in document profiles for target {target} but does not exist on disk.')
                
            log.debug(f'Validated all document profiles for {target}')
        
            # validate module directories
            dirname = map.get('_module_directory')
            if dirname:
                # validate target dir
                target_dirpath = Path(dirname)
                if not target_dirpath.exists() or not target_dirpath.is_dir():
                    raise FileNotFoundError(f'Module directory `{target_dirpath.resolve()}` does not exist or is not a directory on {target}')
                log.debug(f'Validated {target} module directory at `{target_dirpath.resolve()}`')

                # validate local dir matches
                local_dirpath = Path(self.tldir, target_dirpath.name)
                if not local_dirpath.exists() or not local_dirpath.is_dir():
                    raise FileNotFoundError(f'Module directory `{local_dirpath.resolve()}` does not exist or is not a directory in local git TLD')
                log.debug(f'Validated local module directory at `{local_dirpath.resolve()}`')
    
    def _validate_filemap_schema():
        raise NotImplementedError # TODO:
    
    def _create_filemap(self, filepath: str):
        if UserInputHandler.continue_yN(f'Creating new file map at {filepath}.'):
            with open(filepath, 'w') as file:
                filemap_template = self._get_filemap_template()   
                json.dump(filemap_template, file, indent=4)

                raise ValueError(f'Initialize keys and values in new template file map created at ${filepath}')
        else:
            raise FileNotFoundError(f'Create a file map at {filepath} before continuing.')

    def _get_filemap_template(self):
        return {
            "_targets": {
                "<target name>": {
                    "_document_profiles": {
                        "<filename>": "<document profile id>"
                    },
                    "_module_directory": "<module directory path>"
                }
            }
        }

class Command(ABC):
    """
    The base class inherited by every command subclass. 
    Initializes common attributes like file map and API adaptors for each endpoint.
    """
    def __init__(self, filemap, command, targets, username, password, **args):
        if not username and config.has_option('general', 'default_username') and config['general']['default_username']:
            username = config['general']['default_username']
            log.info(f'Using default username {username} from `config.ini`')
        
        username = UserInputHandler.get_username_if_none(username)
        password = UserInputHandler.get_password_if_none(password)

        self.command: str = command
        self.filemap: FileMap = filemap
        self.api_connections: Dict[str, APIAdaptor] = self._init_api_connections(targets, username, password)

    @abstractmethod
    def execute(self):
        '''Execute the commmand with provided arguments.'''
        pass
    
    @staticmethod
    @abstractmethod
    def add_arguments(subparser: ArgumentParser):
        '''Add command-specific arguments in this method.'''
        pass

    def has_uncommitted_changes(self):
        status = run_cli_command(['git', 'status', '--porcelain'])
        return len(status) > 0

    def _init_api_connections(self, targets, username, password):
        api_connections = {}

        for target in targets:
            target_url = config['urls'][target]
            target_webservice_id = config['webservice_ids'][target]

            api_connections[target] = APIAdaptor(target_url, target_webservice_id, username, password)
        
        return api_connections

class PushCommand(Command):
    """
    Pushes the files listed in the file map to the AiM document repository.
    """
    def __init__(self, filemap, **kwargs):
        super().__init__(filemap, **kwargs)

        self.allow_uncommitted: bool = kwargs.get('allow_uncommitted')
        self.version: str = kwargs.get('version')

    def execute(self):
        log.debug(f'Executing {self.command} command with connections {list(self.api_connections.keys())}')
        if not self._commit_state_is_valid(): 
            return # exit if we're not ready to send yet
        
        target_responses = self._send_all()
        if target_responses:
            processed_responses = self._remap_target_responses(target_responses)
            self._create_git_tags(processed_responses)

    @staticmethod
    def add_arguments(subparser):
        subparser.add_argument('--allow-uncommitted', '-a', required=False, action='store_true', help='Allow pushing files to document repository with uncommitted changes')
        subparser.add_argument('--version', '-V', required=False, choices=['major', 'minor'], default='minor', help='Specify the version type that will be stored in the document repository')

    def _remap_target_responses(self, target_responses:Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
        """
        Map target's document ID back to filenames before we tag this push.
        """
        responses = target_responses.copy()

        for target, doc_map in responses.items():
            profiles = self.filemap.get_document_profiles(target)
            reversed_profiles = {local_doc_id: filename for filename, local_doc_id in profiles.items()}
            new_doc_map = {} # avoid runtime error since we can't modify doc_map during iteration

            for doc_id, doc_ver_map in doc_map.items():
                if doc_id in reversed_profiles:
                    filename = reversed_profiles[doc_id]
                    new_doc_map[filename] = doc_ver_map
                else:
                    new_doc_map[doc_id] = doc_ver_map
            responses[target] = new_doc_map

        return responses

    def _create_git_tags(self, target_responses:Dict[str, Dict]):
        """
        Create a Git tag with the name `push.<targetname1>.<timestamp>`, where there can be an 
        arbitrary number of targets separated by `-`, and where `<timestamp>` follows 
        the format `%Y%m%dT%H%M%S`. The tag body contains a mapping of local filenames to 
        the corresponding server document version IDs.
        """
        targets = [key for key in target_responses]
        target_names = '-'.join(targets)
        now = datetime.now().isoformat()
        now_no_delim = now.replace('-', '').replace(':', '').split('.')[0]

        tag_name = f'{self.command}.{target_names}.{now_no_delim}' # TODO: validate git tag name
        tag_msg = json.dumps(target_responses, indent=4)

        run_cli_command(['git', 'tag', '-a', tag_name, '-m', tag_msg])

    def _send_all(self) -> Dict[str, Dict]:
        """
        Send files to all targets in self.api_connections and return a dictionary of targets and responses.
        """
        client_data = self._get_client_data()
        target_responses = {}

        for target, api in self.api_connections.items():
            if not UserInputHandler.continue_Yn(f'Sending files to {target}.'): 
                continue # skip to next target if user doesn't want to continue

            try:
                response = self._post_files_to_target(target, api, client_data)
                self._copy_files_to_target(target, client_data)
            except requests.exceptions.RequestException as e:
                log.error(f'Error pushing files to endpoint {api.url}: {e}')
            except (FileNotFoundError, IsADirectoryError, NotADirectoryError, OSError) as e:
                log.error(f'Error copying module files to {target}: {e}')

            if response:
                target_responses[target] = response
            else:
                log.error(f'No response from target: {target}')
        
        return target_responses
    
    def _copy_files_to_target(self, target: str, client_data: Dict): # TODO: temporary, do this through the webservice
        target_dirname = self.filemap.get_module_directory(target)
        if not target_dirname:
            log.info(f'No modules to copy: `_module_directory` filemap key was null')
            return # exit

        target_dir = Path(target_dirname)
        source_dir = Path(self.filemap.tldir, target_dir.name)

        # remove files
        for item in target_dir.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
            log.debug(f'Removed {item} from {target}')
        
        # copy files
        for item in source_dir.iterdir():
            if item.is_dir():
                shutil.copytree(item, target_dir / item.name)
            else:
                shutil.copy2(item, target_dir / item.name)
            log.debug(f'Copied {item} to {target}')
        
        # add commit
        commit_filename = client_data['current_sha_hash'] + '.commit'
        commit_filepath = target_dir / commit_filename

        with open(commit_filepath, 'w') as file:
            file.write(json.dumps(client_data, indent=4))
            log.debug(f'Created .commit file in {target}')

    def _post_files_to_target(self, target:str, api:APIAdaptor, client_data:Dict) -> Union[Dict, None]:
        """
        Send files and client git data to a specific target endpoint, parse and return response.
        """
        profiles = self.filemap.get_document_profiles(target)
        mapped_files = self.filemap.get_mapped_files(profiles)

        mapped_files['client_data'] = (None, json.dumps(client_data), 'application/json')
        try:
            response = api.post_files(self.command, files=mapped_files)
            return response
        except requests.exceptions.RequestException as e:
            log.error(f'Error pushing files to endpoint {api.url}: {e}')
            return None

    def _get_client_data(self) -> Dict[str, str]:
        """
        Identifies the local branch, commit, and check-in version (major/minor) to the server endpoint.
        Any changes to the data dictionary must be reflected in the server plugin.
        """
        current_sha = run_cli_command(['git', 'rev-parse', 'HEAD'])
        current_branch = run_cli_command(['git', 'rev-parse', '--abbrev-ref', 'HEAD'])
        current_commit_msg = run_cli_command(['git', 'log', '-1', '--pretty=%B'])

        return {
            "current_branch": current_branch,
            "current_sha_hash": current_sha,
            "current_commit_msg": current_commit_msg,
            "version_type": self.version
        }
    
    def _commit_state_is_valid(self):
        if self.has_uncommitted_changes() and not self.allow_uncommitted:
            log.error(f'Git has uncommitted changes, please commit and try again, or use --allow-uncommitted flag.')
            return False
        return True

class ShowCommand(Command):
    """
    Shows the branch and versions of all mapped files checked into the AiM document repository. 
    """
    def __init__(self, filemap, **kwargs):
        super().__init__(filemap, **kwargs)
    
    def execute(self):
        target_responses = self._get_current_versions()

        if target_responses:
            json_string = json.dumps(target_responses, indent=4)
            formatted_string = json_string \
                .replace('[', '') \
                .replace(']', '') \
                .replace('{', '') \
                .replace('}', '') \
                .replace('\\"', '') \
                .replace('"', '') \
                .replace(',\n', '\n') \
                .strip()
            print(formatted_string)
    
    @staticmethod
    def add_arguments(subparser):
        subparser.add_argument('--check-synced', required=False, action='store_true', help="Check current documents in targets' document repository are from the most recent script push.")

    def _get_current_versions(self) -> Dict[str, dict]: 
        target_responses = {}

        for target, api in self.api_connections.items():
            response = self._get_current_versions_from_target(target, api)
            if response:
                target_responses[target] = response
            else:
                log.error(f'No response from target: {target}')   
        
        return target_responses
    
    def _get_current_versions_from_target(self, target: str, api: APIAdaptor) -> Dict[str, Dict]:
        profiles = self.filemap.get_document_profiles(target)
        dto_name = 'AeDocumentVersion'
        version_map = {}

        for filename, doc_id in profiles.items():
            key_query = FindListQueryBuilder(0, 1) # getting most recent doc version only
            key_query.add_attribute(AttributeEquals('docId', doc_id))
            key_query.add_column_spec(ColumnSpecification('docId'))
            key_query.add_column_spec(ColumnSpecification('docVerId'))
            key_query.add_column_spec(ColumnSpecification('versionLabel'))
            key_query.add_column_spec(ColumnSpecification('editDate', SortDirectionEnum.DESCENDING))
            key_query.add_column_spec(ColumnSpecification('checkedInBy'))
            key_query.add_column_spec(ColumnSpecification('checkedInComment'))
            # key_query.add_column_spec(ColumnSpecification('mimeType'))
            # key_query.add_column_spec(ColumnSpecification('byteLength'))
            key_query.add_column_spec(ColumnSpecification('contentUrl'))

            try:
                query_results = api.find_list(dto_name, key_query.to_dict())
                dto_results = query_results.get('results', None)
                doc_ver_keys = dto_results[0] if dto_results else None

                if doc_ver_keys:
                    version_map[filename] = doc_ver_keys

                    # doc_ver_keys.pop('editDate') # remove non-primary key

                    # doc_ver_dto = api.find_hierarchy(dto_name, doc_ver_keys, True)
                    # version_map[filename] = doc_ver_dto
            except requests.exceptions.RequestException as e:
                log.error(f'Error getting most recent version of {filename} from API: {e}')
        
        return version_map
    
class PullCommand(Command):
    """
    Pulls the files listed in the file map from the AiM document repository.
    """
    def __init__(self, filemap, **kwargs):
        super().__init__(filemap, **kwargs)
    
    def execute(self):
        raise NotImplementedError
    
    @staticmethod
    def add_arguments(subparser):
        pass

class CommandParser:
    def __init__(self, filemap):
        self.filemap = filemap

        self.parser = argparse.ArgumentParser(
            description="Two-part custom interface between a local git instance and the AiM Document Repository. "
            "All operations are done using the filemap file, which maps AiM document ID's to local files."
        )
        self.subparsers = self.parser.add_subparsers(dest='command')
    
        self.commands = {
            'push': PushCommand,
            'pull': PullCommand,
            'show': ShowCommand
        }
        self._init_commands()
    
    def _init_commands(self):
        for command_name, command_class in self.commands.items():
            subparser = self.subparsers.add_parser(command_name, help=command_class.__doc__)

            """ add default arguments for every command """
            subparser.add_argument(
                '--targets', '-t', required=True, choices=config.options('urls'), nargs='+', 
                help="One or more server names to target; URLs and webservice ID's must be setup in the scripts `config.ini` file"
            )
            subparser.add_argument(
                '--username', '-u', required=False, 
                help="Username will be attached to new document versions at the `editClerk` and `checkedInBy` fields"
            )
            subparser.add_argument(
                '--password', '-p', required=False, 
                help="CAUTION: password passed as argument may persist in terminal history; Clear session with `history -c`"
            )

            """ add arguments specific to a command instance """
            command_class.add_arguments(subparser)
    
    def parse_args(self) -> Command:
        args = self.parser.parse_args()

        if args.command in self.commands:
            command_class = self.commands[args.command]
            command = command_class(self.filemap, **vars(args))
            return command
        else:
            raise ValueError(f'Invalid command: [{args.command}]')

def run_cli_command(cli_command:List[str], cwd=None) -> Union[str, None]:
    try:
        if not isinstance(cli_command, list):
            raise ValueError(f'CLI command must be in list form')
        
        result = subprocess.run(
            cli_command, 
            cwd=cwd, 
            check=True, 
            text=True, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        log.error(f'Unable to run command: {cli_command}')
        log.error(f'stderr: {e.stderr}')
    except ValueError as e:
        log.error(f'Invalid input: {e}')
    except Exception as e:
        log.error(f'Unexpected exception: {e}')

    return None

def main():
    try:
        filemap = FileMap(MAP_FILENAME)
    except (FileNotFoundError, ValueError) as e:
        log.error(e)
        log.error(f'Fix file map errors')
        sys.exit(1)

    command_parser = CommandParser(filemap)
    try: 
        command = command_parser.parse_args()
    except ValueError as e:
        log.error(e)
        command_parser.parser.print_help()
    
    try:
        command.execute()
    except:
        log.error(e)

if __name__ == '__main__':
    try:
        log.debug('Script started')
        main()
        log.debug('Script ended')
    except KeyboardInterrupt: 
        log.info('Script stopped by keyboard interrupt')