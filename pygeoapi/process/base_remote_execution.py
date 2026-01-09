# =================================================================
#
# Authors: Francesco Martinelli <francesco.martinelli@ingv.it>
#
# Copyright (c) 2024 Francesco Martinelli
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import logging
import os
from typing import Any, Optional, Tuple
import requests
import shutil
import time

from pathlib import Path
from urllib.parse import urljoin

from pygeoapi.process.base import (
    BaseProcessor,
    ProcessorExecuteError,
    ProcessorGenericError,
)

LOGGER = logging.getLogger(__name__)


class BaseRemoteExecutionProcessor(BaseProcessor):
    """Generic Processor to execute remotely """
    def __init__(self, processor_def: dict, process_metadata: dict):
        """
        Initialize object

        :param processor_def: processor definition
        :param process_metadata: process metadata `dict`

        :returns:
            pygeoapi.processor.base_remote_execution.
                BaseRemoteExecutionProcessor
        """
        super().__init__(processor_def, process_metadata)
        self.private_processor_dir = processor_def['private_processor_dir']
        self.url_executor = processor_def['url_executor']

        if self.private_processor_dir is None:
            raise ProcessorGenericError(
                'Undefined \'private_processor_dir\' in configuration.')
        try:
            os.mkdir(self.private_processor_dir, mode=0o755)
        except FileExistsError:
            pass

        if self.url_executor is None:
            raise ProcessorGenericError(
                'Undefined \'url_executor\' in configuration.')

        self.private_processor_dir = Path(self.private_processor_dir)

        self.polling_time = processor_def.get('polling_time', 3)
#        self.max_waiting_loops = max(
#            0, int(processor_def.get('max_waiting_loops', 1))
#        )

        self.remote_execute_synch = processor_def.get(
            'remote_execute_synch', True
        )
        self.job_id = None

    def set_job_id(self, job_id: str) -> None:
        self.job_id = job_id

    def prepare_input(self, data, working_dir, outputs):
        """
        validate the input and prepare the objet to send to the 'code'

        :param data: inputs data received by the caller.
        Data must be checked they match the metadata definition of 'inputs'.

        NOTE: the logic to pass the received parameters to the 'code' is up
        to the specialised class.

        NOTE: specifically, it is valid also for the "file(s) passed as
        parameter(s) as encoded string":
        it is up to the specialised class to handle them to the 'code'.
        Options could be:
        -) get and save the file(s) to the working directory,
        and pass to the 'code' the filename as parameter;
        -) transfer to the 'code' the files as streams;
        -) other...

        Note:
        *****
        The 'code' may produce a reference (i.e. an URL) instead of a value:

        se ci deve essere della logica sull'output da produrre
        (value Vs. reference) deve essere gestita nella classe specifica,
        sia trasmettendo al 'code' informazioni adeguate,
        sia salvando in locale informazioni da utilizzare al momento di
        preparare l'outout.

        Return the dictionary with the parameters to be passed to the 'code'.
        """
        raise NotImplementedError()

    def prepare_output(self, data, working_dir, outputs):
        """
        prepare the output object as defined by the metadata definition of
        'outputs'.

        :param data: data dictionary received back from the 'code'

        NOTE: the logic to pass back the data received from the 'code' is up
        to the specialised class.

        NOTE: specifically, it is also valid for file(s) produced by the
        'code': the specialised class must handle the returned data to
        produce the 'outputs' as defined by the metadata definition.

        NOTE: if the 'code' returns an URL to access a file, the function can
        return the URL as string, "ONLY IF" the 'outputs' defined a string.

        NOTE: to have the file returned to the caller as reference, the
        function must return the encoded string; the manager will save the
        encoded string as file and return:
        -) "response_headers" with the reference of the file location
        -) "outputs" empty

        Return the outputs dictionary.
        """
        raise NotImplementedError()

    def execute(self, data: dict, outputs: Optional[dict] = None
                ) -> Tuple[str, Any]:
        if not self.job_id:
            # should be happen only in testing
            raise ProcessorGenericError(
                'Missing call to \'set_job_id()\' before \'execute()\'.')

        working_dir = str(self.private_processor_dir / self.job_id)
        os.mkdir(working_dir, mode=0o755)

        try:
            code_input_params = self.prepare_input(data, working_dir, outputs)
        except BaseException as ex:
            shutil.rmtree(working_dir)
            raise ex

        # Call the processing server, always synch:
        execute_url = urljoin(self.url_executor, "execute")
        headers = {'Content-type': 'application/json'}
        response = requests.post(execute_url, json={
          'application_params': {
              'job_id': self.job_id,
              'synch_execution': self.remote_execute_synch
          },
          'code_input_params': code_input_params},
          headers=headers
        )
        if not response.ok:
            try:
                # Unaccepted request: the dir and files are useless:
                shutil.rmtree(working_dir)
                # Get returned message
                message = response.json()['Message']
                raise ProcessorExecuteError(message)
            except Exception:
                # If no returned message, get exception message
                raise ProcessorExecuteError(response)

        # Nota: siccome response.ok, allora il thread è sicuramente partito,
        # alternativamente avrebbe risposto con un abort().

        if self.remote_execute_synch:
            info = response.json()
        else:
            # Aspetta attivamente (con sleep) che il 'code' sia terminato
#            max_waiting_loops = self.max_waiting_loops + 1
#            while (max_waiting_loops := max_waiting_loops-1) > 0:
            while True:
                time.sleep(self.polling_time)
                execute_url = urljoin(
                    self.url_executor, "job_info/" + self.job_id
                )
                response = requests.get(execute_url)
                if not response.ok:
                    try:
                        message = response.json()['Message']
                        raise ProcessorExecuteError(message)
                    except Exception:
                        raise ProcessorExecuteError(response)
                    
                info = response.json()
                if info['job_info']['end_processing']:
                    break

        if info['job_info']['exit_code'] != 0:
            error_msg = (
                f"The job '{info['job_id']}' exited with code: "
                f"{info['job_info']['exit_code']}\n"
                f"Error message:\n{info['job_info']['std_err']}"
            )
            logging.error(error_msg)
            # Don't return complete message to client:
            # could contain security info.
            message = (
                f"The job '{info['job_id']}' "
                f"exited with code {info['job_info']['exit_code']}"
            )
            raise ProcessorExecuteError(message)
        
        mimetype, process_outputs = self.prepare_output(info, working_dir, outputs)

        return mimetype, process_outputs

    def __repr__(self):
        return f'<BaseRemoteExecutionProcessor> {self.name}'
