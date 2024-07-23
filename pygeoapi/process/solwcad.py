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
import re

from pathlib import Path

from pygeoapi.process.base import (
    ProcessorExecuteError,
    #    ProcessorGenericError,
)
from pygeoapi.process.base_remote_execution import BaseRemoteExecutionProcessor

LOGGER = logging.getLogger(__name__)

#: Process metadata and description
PROCESS_METADATA = {
    'id': 'solwcad',
    'title': 'SOLWCAD',
    'description':
        'Fortran code to compute the saturation surface of H2O-CO2 '
        'fluids in silicate melts of arbitrary composition.',
    'version': '1.0.0',
    'jobControlOptions': [
        'async-execute',
        'sync-execute'
    ],
    'keywords': ['Fortran code', 'saturation surface', 'other keywords...'],
    'inputs': {
        'swinput.data': {
            'title': 'Desired computation',
            'description': 'Specifics for the desired computation.',
            'minOccurs': 1,
            'maxOccurs': 1,
            'schema': {
                'oneOf': [
                    {
                        'type': 'object',
                        'description':
                            'The computation is performed at user-defined P-T '
                            'conditions in sw.data. H2O and CO2 contents in '
                            'sw.data refer to total amounts in the two-phase '
                            'magma, or to (mass of volatile component) / '
                            '(mass of melt + fluid phases). '
                            'SOLWCAD computes the partitioning of the two '
                            'volatiles in the fluid and melt phases for '
                            'user-defined composition. '
                            'Computations are performed from item ndat1 to '
                            'item ndat2 (only one computation is performed if '
                            'ndat1 = ndat2).',
                        'required': [
                            'ndat1',
                            'ndat2',
                            'kl'
                        ],
                        'properties': {
                            'ndat1': {
                                'type': 'integer',
                                'description':
                                    'Computations are performed from item '
                                    'ndat1 of sw.data'
                              },
                            'ndat2': {
                                'type': 'integer',
                                'description':
                                    'Computations are performed up to item '
                                    'ndat2 of sw.data'
                            },
                            'kl': {
                                'type': 'integer',
                                'enum': [0]  # the only one accepted value
                            }
                            # not used for kl = 0:
                            # 'iopen', 'fopen', 'dt', 'tlimit'
                        }
                    },
                    {
                        'type': 'object',
                        'description':
                            'The computation is performed with reference to '
                            'item ndat1 in sw.data, at constant user-defined '
                            'T and for pressure from user-defined P to '
                            'atmospheric. At each pressure, a computation '
                            'similar to the one for kl=0 is performed.',
                        'required': [
                            'ndat1',
                            'kl',
                            'iopen'
                        ],
                        'properties': {
                            'ndat1': {
                                'type': 'integer',
                                'description':
                                    'Computations are performed from item '
                                    'ndat1 of sw.data'
                            },
                            'kl': {
                                'type': 'integer',
                                'enum': [1],  # the only one accepted value
                            },
                            'iopen': {
                                'type': 'integer',
                                'enum': [0, 1],
                                'description':
                                    '0 for closed-system calculations, '
                                    '1 for open system calculations.'
                            },
                            'fopen': {
                                'type': 'string',
                                'pattern':
                                    r"^([+-]?([\d]+\.|[\d]*\.[\d]+))"
                                    r"([Dd][+-]?[\d]+)?$",
                                'description':
                                    'Only used with iopen =1. It specifies '
                                    'the weight fraction of fluid phase lost '
                                    'at each subsequent computation step.'
                            }
                            # not used for kl = 0:
                            # 'dt', 'tlimit'
                        }
                    },
                    {
                        'type': 'object',
                        'description':
                            'Same as for kl=1, but for fixed P (from sw.data) '
                            'and T from the item ndat1 in sw.data to a '
                            'user-defined value tlimit, with user-defined '
                            'T-steps.',
                        'required': [
                            'ndat1',
                            'kl',
                            'iopen',
                            'dt',
                            'tlimit'
                            # 'fopen' is not always required, only if iopen=1
                        ],
                        'properties': {
                            'ndat1': {
                                'type': 'integer',
                                'description':
                                    'Computations are performed on item ndat1 '
                                    'of sw.data'
                            },
                            'kl': {
                                'type': 'integer',
                                'enum': [2],  # the only one accepted value
                            },
                            'iopen': {
                                'type': 'integer',
                                'enum': [0, 1],
                                'description':
                                    '0 for closed-system calculations, '
                                    '1 for open system calculations.'
                            },
                            'fopen': {
                                'type': 'string',
                                'pattern':
                                    r"^([+-]?([\d]+\.|[\d]*\.[\d]+))"
                                    r"([Dd][+-]?[\d]+)?$",
                                'description':
                                    'Only used with iopen =1. It specifies '
                                    'the weight fraction of fluid phase lost '
                                    'at each subsequent computation step.'
                            },
                            'dt': {
                                'type': 'string',
                                'pattern':
                                    r"^([+-]?([\d]+\.|[\d]*\.[\d]+))"
                                    r"([Dd][+-]?[\d]+)?$",
                                'description':
                                    'The length of the T-steps (either '
                                    'positive or negative).'
                            },
                            'tlimit': {
                                'type': 'string',
                                'pattern':
                                    r"^([+-]?([\d]+\.|[\d]*\.[\d]+))"
                                    r"([Dd][+-]?[\d]+)?$",
                                'description':
                                    'The temperature up to which separate '
                                    'computations are performed. It can be '
                                    'either higher (dt>0) or lower (dt<0) '
                                    'than T.'
                            }
                        }
                    },
                    {
                        'type': 'object',
                        'description':
                            'H2O and CO2 in sw.data items represent amounts '
                            'dissolved in the melt phase. For user-defined '
                            'melt composition and temperature, SOLWCAD '
                            'returns the equilibrium pressure and composition '
                            'of the coexisting fluid phase. Computations are '
                            'performed from item ndat1 to item ndat2 (only '
                            'one computation is performed if ndat1=ndat2 ). '
                            'This kind of computation is commonly used in the '
                            'analysis of melt inclusion data.',
                        'required': [
                            'ndat1',
                            'ndat2',
                            'kl'
                        ],
                        'properties': {
                            'ndat1': {
                                'type': 'integer',
                                'description':
                                    'Computations are performed on item ndat1 '
                                    'of sw.data'
                            },
                            'ndat2': {
                                'type': 'integer',
                                'description':
                                    'Computations are performed up to item '
                                    'ndat2 of sw.data'
                            },
                            'kl': {
                                'type': 'integer',
                                'enum': [-1],   # the only one accepted value
                            }
                            # not used for kl = 0:
                            # 'iopen', 'fopen', 'dt', 'tlimit'
                        }
                    }
                ]
            }
        },
        'sw.data': {
            'title': 'User data',
            'description':
                'user-defined conditions in terms of pressure, temperature, '
                'and composition, each one arranged on a single item. Each '
                'item contains the followings: pressure (Pa); '
                'temperature (K); H2O content (wt fraction)**; '
                'CO2 content (wt fraction)**; the following ten quantities '
                'specify the volatile-free melt composition (wt fraction)***, '
                'in the following order: SiO2; TiO2; Al2O3; Fe2O3; FeO; MnO; '
                'MgO; CaO; Na2O; K2O. '
                '**H2O and CO2 contents may refer to i) total amounts in the '
                'two-phase magma, equal to (mass of volatile in the '
                'fluid + mass of volatile dissolved in the melt) / (mass of '
                'the gas phase + mass of the melt phase); or ii) the amounts '
                'dissolved in the melt phase, that is, (mass of volatile '
                'dissolved in the melt) / (mass of the melt phase). The '
                'specific choice is determined by the parameter kl in '
                'swinput.data.',
            'minOccurs': 1,
            'maxOccurs': 'unbounded',
            'schema': {
                'type': 'array',
                'minItems': 14,
                'maxItems': 14,
                'items': {
                    'type': 'string',
                    'pattern':
                        r"^([+-]?([\d]+\.|[\d]*\.[\d]+))"
                        r"([Dd][+-]?[\d]+)?$",
                }
            }
        }
    },
    'outputs': {
        'title': 'Output result',
        'solwcad.out': {
            'description':
                'Each record item contains the following quantities: '
                'Pressure (Mpa); Temperature (K); Total ( kl >0) or '
                'dissolved ( kl =-1) H2O (wt%); '
                'Total ( kl >0) or dissolved ( kl =-1) CO2 (wt%); '
                'H2O dissolved in the melt (wt%); '
                'CO2 dissolved in the melt (ppm); CO2 in the fluid (wt%); '
                'CO2 in the fluid (mol%); Amount of fluid phase '
                'in magma (wt%); Amount of fluid phase in magma (vol%); '
                'Density of the melt phase (kg/m3 ); Density of the gas '
                'phase (kg/m3 ); Density of the two-phase magma (kg/m3 ); '
                'Viscosity of the melt phase [log (Pa s)]; Viscosity of '
                'the two-phase magma [log (Pa s)].',
            'minOccurs': 1,
            'maxOccurs': 'unbounded',
            'schema': {
                'type': 'array',
                'minItems': 15,
                'maxItems': 15,
                'items': {
                    'type': 'string',
                    'pattern':
                        r"^([+-]?(?:[[:digit:]]+\.|[[:digit:]]*\."
                        r"[[:digit:]]+))(?:[Dd][+-]?[[:digit:]]+)?$"
                }
            }
        }
        #    ,
        #    'solwcad.json': {
        #      'description':
        #        'An array of objects, each containing '
        #        'the calculated properties.',
        #      'minOccurs': 1,
        #      'maxOccurs': 'unbounded',
        #      'schema': {
        #        'type': 'object',
        #        'properties': {
        #          'Pressure (Mpa)': {
        #            'type': 'number'
        #          },
        #          'Temperature (K)': {
        #            'type': 'number'
        #          },
        #          'H2O (wt%)': {
        #            'description': 'Total ( kl >0) or dissolved ( kl =-1)',
        #            'type': 'number'
        #          },
        #          'CO2 (wt%)': {
        #            'description': 'Total ( kl >0) or dissolved ( kl =-1)',
        #            'type': 'number'
        #          },
        #          'H2O dissolved in the melt (wt%)': {
        #            'type': 'number'
        #          },
        #          'CO2 dissolved in the melt (ppm)': {
        #            'type': 'number'
        #          },
        #          'CO2 in the fluid (wt%)': {
        #            'type': 'number'
        #          },
        #          'CO2 in the fluid (mol%)': {
        #            'type': 'number'
        #          },
        #          'Amount of fluid phase in magma (wt%)': {
        #            'type': 'number'
        #          },
        #          'Amount of fluid phase in magma (vol%)': {
        #            'type': 'number'
        #          },
        #          'Density of the melt phase (kg/m3)': {
        #            'description':
        #              'melt density is computed by the Lange (1994) model',
        #            'type': 'number'
        #          },
        #          'Density of the gas phase (kg/m3)': {
        #            'type': 'number'
        #          },
        #          'Density of the two-phase magma (kg/m3)': {
        #            'type': 'number'
        #          },
        #          'Viscosity of the melt phase [log (Pa s)]': {
        #            'description':
        #              'melt viscosity is computed by '
        #              'the Giordano et al. (2008) model',
        #            'type': 'number'
        #          },
        #          'Viscosity of the two-phase magma [log (Pa s)]': {
        #            'description':
        #              'the viscosity of bubble-bearing melt is computed by '
        #              'the Ishii and Zuber (1979) model for non-deformable '
        #              'bubbles',
        #            'type': 'number'
        #          }
        #        }
        #      }
        #    }
    },
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://example.org/process',
        'hreflang': 'en-US'
      }],
    'example': {
        'inputs': {
            'swinput.data': {
                'value': {'ndat1': 1, 'ndat2': 2, 'kl': 0}
            },
            'sw.data': [{
                    'value': [
                        '1.00d8', '1273.', '.0400', '.0200', '.7653',
                        '.0032', '.1201', '.0027', '.0246', '.0006',
                        '.0018', '.0132', '.0378', '.0306'
                    ]
                },
                {
                    'value': [
                        '2.00d8', '1173.', '.0200', '.0010', '.7053',
                        '.0032', '.1301', '.0027', '.0146', '.0006',
                        '.0118', '.0232', '.0378', '.0306'
                    ]
                }
            ]
        }
    }
    # curl localhost:5000/processes/solwcad/execution
    #     -H 'Content-Type: application/json'
    #     -d '{ "inputs" : {  "swinput.data" : { "value" :
    #         { "ndat1" : 1 , "ndat2" : 2 , "kl" : 0 } },
    #         "sw.data" : [ { "value" : [ "1.00d8" , "1273." , ".0400" ,
    #         ".0200" , ".7653" , ".0032" , ".1201" , ".0027" , ".0246" ,
    #         ".0006" , ".0018" , ".0132" , ".0378" , ".0306" ] },
    #         { "value" : [ "2.00d8" , "1173." , ".0200" , ".0010" ,
    #         ".7053" , ".0032" , ".1301" , ".0027" , ".0146" , ".0006" ,
    #         ".0118" , ".0232" , ".0378" , ".0306" ] } ] }
}


class SolwcadProcessor(BaseRemoteExecutionProcessor):
    """Solwcad Processor example"""
    def __init__(self, processor_def):
        """
        Initialize object
        :param processor_def: provider definition

        :returns: pygeoapi.process.solwcad.SolwcadProcessor
        """
        super().__init__(processor_def, PROCESS_METADATA)

    def prepare_output(self, info, working_dir):
        # Only one output:
        #   "output in requested format"
        #   mediatype "as per output definition from process description"

        # Note: the 'code' may return a URL instead of the value.
        # but in this case the "product" is a string in URL format,
        # NOT the object/file that can be retrieved at the given URL.

        mimetype = 'application/json'

        code_params = info['params']
        solwcad_out = []
        with open(str(Path(working_dir) / code_params['-output']), mode='r+t'
                  ) as output_file:
            # NOTE: there is no check the output is well formatted,
            # i.e. one line per set of 15 numbers, without empty lines
            while len(line_items := output_file.readline().strip('\n')) > 0:
                solwcad_out.append({"value": line_items.split()})

        return mimetype, solwcad_out

    def prepare_input(self, data, working_dir):
        pattern_generic_number = \
            r"^([+-]?([\d]+\.|[\d]*\.[\d]+))([Dd][+-]?[\d]+)?$"

        try:
            swinput = data['swinput.data']['value']
            sw = []
            for item in data['sw.data']:
                sw.append(item['value'])
        except Exception as err:
            err_msg = 'Input not correctly formatted: ' + str(err) + '\'.'
            raise ProcessorExecuteError(err_msg)

        kl = swinput.get('kl', None)
        if kl is None:
            err_msg = 'Value \'swinput.data[\'kl\']\' must be provided.'
            raise ProcessorExecuteError(err_msg)

        ndat1 = swinput.get('ndat1', None)
        ndat2 = swinput.get('ndat2', None)
        iopen = swinput.get('iopen', None)
        fopen = swinput.get('fopen', None)
        dt = swinput.get('dt', None)
        tlimit = swinput.get('tlimit', None)
        match kl:
            case 0 | -1:
                try:
                    int(ndat1)
                    int(ndat2)
                except (ValueError, TypeError):
                    err_msg = 'Values \'swinput.data[\'ndat1\']\' and ' \
                              ' \'swinput.data[\'ndat2\']\' must be integer.'
                    raise ProcessorExecuteError(err_msg)
                swinput['iopen'] = 0
                swinput['fopen'] = swinput['dt'] = swinput['tlimit'] = "0.0"
            case 1:
                try:
                    swinput['iopen'] = iopen = int(iopen)
                    if ((iopen < 0) or (iopen > 1)):
                        err_msg = 'Value \'swinput.data[\'iopen\']\' ' \
                                  'must be [0, 1].'
                        raise ProcessorExecuteError(err_msg)
                except (ValueError, TypeError):
                    err_msg = 'Value \'swinput.data[\'iopen\']\' ' \
                              'must be integer.'
                    raise ProcessorExecuteError(err_msg)

                swinput['fopen'] = fopen = str(fopen)
                if (iopen == 1):
                    if not re.match(pattern_generic_number, fopen):
                        err_msg = 'Value \'swinput.data[\'fopen\']\' ' \
                                  'not correctly formatted.'
                else:
                    swinput['fopen'] = "0.0"

                swinput['ndat2'] = 0
                swinput['dt'] = swinput['tlimit'] = "0.0"
            case 2:
                try:
                    swinput['iopen'] = iopen = int(iopen)
                    if ((iopen < 0) or (iopen > 1)):
                        err_msg = 'Value \'swinput.data[\'iopen\']\' ' \
                                  'must be [0, 1].'
                        raise ProcessorExecuteError(err_msg)
                except (ValueError, TypeError):
                    err_msg = 'Value \'swinput.data[\'iopen\']\' ' \
                              'must be integer.'
                    raise ProcessorExecuteError(err_msg)

                swinput['fopen'] = fopen = str(fopen)
                if (iopen == 1):
                    if not re.match(pattern_generic_number, fopen):
                        err_msg = 'Value \'swinput.data[\'fopen\']\' ' \
                                  'not correctly formatted.'
                else:
                    swinput['fopen'] = "0.0"

                swinput['dt'] = dt = str(dt)
                if not re.match(pattern_generic_number, dt):
                    err_msg = 'Value \'swinput.data[\'dt\']\' ' \
                              'not correctly formatted.'

                swinput['tlimit'] = tlimit = str(tlimit)
                if not re.match(pattern_generic_number, tlimit):
                    err_msg = 'Value \'swinput.data[\'tlimit\']\' ' \
                              'not correctly formatted.'

                swinput['ndat2'] = 0
            case _:
                err_msg = 'Value \'swinput.data[\'kl\']\' not accepted.'
                raise ProcessorExecuteError(err_msg)

        if not isinstance(sw, list | tuple):
            err_msg = 'Received \'sw.data\' with wrong format: not a sequence.'
            raise ProcessorExecuteError(err_msg)

        for sw_line in sw:
            if not isinstance(sw_line, list | tuple):
                err_msg = 'In \'sw.data\' received an item ' \
                          'with wrong format: not a sequence.'
                raise ProcessorExecuteError(err_msg)

            if len(sw_line) != 14:
                err_msg = 'In \'sw.data\' received an item with ' \
                          'wrong number of items: ' + str(len(sw_line)) + '.'
                raise ProcessorExecuteError(err_msg)

            for number in sw_line:
                if not re.match(pattern_generic_number, str(number)):
                    err_msg = 'Value in \'sw.data\' ' \
                              'not correctly formatted: \'' + number + '\'.'
                    raise ProcessorExecuteError(err_msg)

        # Create input file(s) required to run the 'code'
        # ###############################################
        swinput_filename = "swinput.data"
        # The file must not exist, otherwise there is a problem!
        with open(str(Path(working_dir) / swinput_filename),
                  mode='x+t') as swinput_file:
            swinput_file.write(str(swinput['ndat1']) + '\t')
            swinput_file.write(str(swinput['ndat2']) + '\t')
            swinput_file.write(str(swinput['kl']) + '\t')
            swinput_file.write(str(swinput['iopen']) + '\t')
            swinput_file.write(str(swinput['fopen']) + '\t')
            swinput_file.write(str(swinput['dt']) + '\t')
            swinput_file.write(str(swinput['tlimit']) + '\n')

        sw_filename = "sw.data"
        # The file must not exist, otherwise there is a problem!
        with open(str(Path(working_dir) / sw_filename), mode='x+t') as sw_file:
            for line in sw:
                for value in line:
                    sw_file.write(str(value) + '\t')
                sw_file.write('\n')

        # Create the dictionary with the properties to be passed to the 'code'
        # where property_name=parameter_name, property_value=parameter_value
        # ###############################################
        code_input_param = {}
        code_input_param['-swinput'] = swinput_filename
        code_input_param['-sw'] = sw_filename
        code_input_param['-output'] = "output.txt"

        return code_input_param

    def __repr__(self):
        return f'<SolwcadProcessor> {self.name}'
