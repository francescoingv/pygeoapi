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
    # process.yaml -> processSummary.yaml

    # Proprietà required:
    'id': 'solwcad',
    # type string

    'version': '2.1.0',
    # type string

    # Altre proprietà non required:
    'jobControlOptions': [
        'async-execute',
        'sync-execute'
    ],
    'proprieta_aggiunta': 'altra proprietà',
    
    # type: array,
    #   items: {type: string, enum: ['sync-execute', 'async-execute', 'dismiss']}

    # outputTransmission
    # type: array, 
    #   items: {type: string, enum: ['value', 'reference'], default: 'value'}

    # links:
    # type: array, 
    #   items: {type: object, required: 'href', properties:
    #       href: type: string
    #       rel: type: string, example: 'service'
    #       type: type: string, example: 'application/json'
    #       hreflang: type: string, example: 'en'
    #       title: type: string

    # process.yaml -> processSummary.yaml -> descriptionType.yml
    # Tutte proprietà non required:
    'title': 'SOLWCAD',
    # type: string

    'description':
        'Fortran code to compute the saturation surface of H2O-CO2 '
        'fluids in silicate melts of arbitrary composition.',
    # type: string
    
    'keywords': ['Fortran code', 'saturation surface', 'other keywords...'],
    # type: array
    #   items: type: string

    # process.yaml
    'inputs': {
        # inputDescription.yaml
        'elaboration_type': {
            'title': 'Desired computation',
            'minOccurs': 1,
            'maxOccurs': 1,
            'schema': {
                'type': 'string',
                'oneOf': [
                    {
                        'const': 'Saturation',
                        'description':
                            'saturation of water and CO2 at given pressure, '
                            'temperature and silicate melt composition '
                            '(described as weight fractions of 10 major oxides: '
                            'SiO2 TiO2 Al2O3 Fe2O3 FeO MnO MgO CaO Na2O K2O)'
                    },
                    {
                        'const': 'Equilibrium',
                        'description':
                            'equilibrium pressure given temperature, silicate '
                            'melt composition (described as weight fractions of '
                            '10 major oxides: SiO2 TiO2 Al2O3 Fe2O3 FeO MnO MgO '
                            'CaO Na2O K2O) and weight fractions of water and '
                            'CO2 dissolved in the melt'
                    }
                ]
            }
        },
        'pressure': {
            'title': 'pressure P [Pa]',
            'description': 'required only for Saturation',
            'minOccurs': 0,
            'maxOccurs': 1,
            'schema': {
                'type': 'object',
                'required': ['low_pressure'],
                'properties': {
                    'low_pressure': {
                        'type': 'number',
                        'title': 'low pressure',
                        'description':
                            'pressure at which calculations need '
                            'to be performed, or low pressure if '
                            'pressure range is required',
                        'minimum': 10e5,
                        'maximum': 10e9
                    },
                    'high_pressure': {
                        'type': 'number',
                        'title': 'high pressure P [Pa]',
                        'description':
                            'high pressure for pressure range '
                            'calculations. Not to be provided for '
                            'single pressure value calculations',
                        'minimum': 10e5,
                        'maximum': 10e9
                    },
                    'step_pressure': {
                        'type': 'number',
                        'title': 'incremental pressure step [Pa]',
                        'description':
                            'step to use for pressure range '
                            'calculations. Not to be provided for '
                            'single pressure value calculations',
                        'minimum': 0.0,
                        'exclusiveMinimum': True, # 0 non ammesso!
                    }
                }
            }
        },
        'temperature': {
            'title': 'temperature T [K]',
            'minOccurs': 1,
            'maxOccurs': 1,
            'schema': {
                'type': 'object',
                'required': ['low_temperature'],
                'properties': {
                    'low_temperature': {
                        'type': 'number',
                        'title': 'low temperature',
                        'description':
                            'temperature at which calculations need '
                            'to be performed, or low temperature if '
                            'temperature range is required',
                        'minimum': 973.0,
                        'maximum': 1573.0
                    },
                    'high_temperature': {
                        'type': 'number',
                        'title': 'high temperature T [K]',
                        'description':
                            'high temperature for temperature range '
                            'calculations. Not to be provided for '
                            'single temperature value calculations',
                        'minimum': 973.0,
                        'maximum': 1573.0
                    },
                    'step_temperature': {
                        'type': 'number',
                        'title': 'incremental temperature step [K]',
                        'description':
                            'step to use for temperature range '
                            'calculations. Not to be provided for '
                            'single temperature value calculations',
                        'minimum': 0.0,
                        'exclusiveMinimum': True, # 0 non ammesso!
                    }
                }
            }
        },
        'water': {
            'title': 'water content',
            'description':
                'total water content (weight fraction) - '
                'mass of water/total mass of the system',
            'minOccurs': 1,
            'maxOccurs': 1,
            'schema': {
                'type': 'object',
                'required': ['low_water'],
                'properties': {
                    'low_water': {
                        'type': 'number',
                        'title': 'low water content',
                        'description':
                            'water content at which calculations need '
                            'to be performed, or low water content if '
                            'water content range is required',
                        'minimum': 10e-6,
                        'maximum': 0.2
                    },
                    'high_water': {
                        'type': 'number',
                        'title': 'high water content',
                        'description':
                            'high water content for water content range '
                            'calculations. Not to be provided for '
                            'single water content value calculations',
                        'minimum': 10e-6,
                        'maximum': 0.2
                    },
                    'step_water': {
                        'type': 'number',
                        'title': 'incremental water content step',
                        'description':
                            'step to use for water content range '
                            'calculations. Not to be provided for '
                            'single water content value calculations',
                        'minimum': 0.0,
                        'exclusiveMinimum': True, # 0 non ammesso!
                    }
                }
            }
        },
        'co2': {
            'title': 'CO2 content',
            'description':
                'total CO2 content (weight fraction) - '
                'mass of CO2/total mass of the system',
            'minOccurs': 1,
            'maxOccurs': 1,
            'schema': {
                'type': 'object',
                'required': ['low_co2'],
                'properties': {
                    'low_co2': {
                        'type': 'number',
                        'title': 'low co2 content',
                        'description':
                            'co2 content at which calculations need '
                            'to be performed, or low co2 content if '
                            'co2 content range is required',
                        'minimum': 10e-6,
                        'maximum': 0.2
                    },
                    'high_co2': {
                        'type': 'number',
                        'title': 'high wateco2 content',
                        'description':
                            'high co2 content for co2 content range '
                            'calculations. Not to be provided for '
                            'single co2 content value calculations',
                        'minimum': 10e-6,
                        'maximum': 0.2
                    },
                    'step_co2': {
                        'type': 'number',
                        'title': 'incremental co2 content step',
                        'description':
                            'step to use for co2 content range '
                            'calculations. Not to be provided for '
                            'single co2 content value calculations',
                        'minimum': 0.0,
                        'exclusiveMinimum': True, # 0 non ammesso!
                    }
                }
            }
        },

        'oxides': {
            'title': 'oxides composition',
            'description':
                'silicate melt composition, described as weight fractions '
                'of 10 major oxides',
            'minOccurs': 1,
            'maxOccurs': 'unbounded',
            'schema': {
                'type': 'array',
                'minItems': 1,
                'maxItems': 'unbounded',
                'items': {
                    'type': 'object',
                    'required': [
                        'sio2', 'tio2', 'al2o3', 'fe2o3', 'feo',
                        'mno', 'mgo', 'cao', 'na2o', 'k2o'
                    ],
                    'properties': {
                        'sio2': {
                            'type': 'number',
                            'title': 'SiO2',
                            'minimum': 0.0,
                            'maximum': 1.0
                        },
                        'tio2': {
                            'type': 'number',
                            'title': 'TiO2',
                            'minimum': 0.0,
                            'maximum': 1.0
                        },
                        'al2o3': {
                            'type': 'number',
                            'title': 'Al2O3',
                            'minimum': 0.0,
                            'maximum': 1.0
                        },
                        'fe2o3': {
                            'type': 'number',
                            'title': 'Fe2O3',
                            'minimum': 0.0,
                            'maximum': 1.0
                        },
                        'feo': {
                            'type': 'number',
                            'title': 'FeO',
                            'minimum': 0.0,
                            'maximum': 1.0
                        },
                        'mno': {
                            'type': 'number',
                            'title': 'MnO',
                            'minimum': 0.0,
                            'maximum': 1.0
                        },
                        'mgo': {
                            'type': 'number',
                            'title': 'MgO',
                            'minimum': 0.0,
                            'maximum': 1.0
                        },
                        'cao': {
                            'type': 'number',
                            'title': 'CaO',
                            'minimum': 0.0,
                            'maximum': 1.0
                        },
                        'na2o': {
                            'type': 'number',
                            'title': 'Na2O',
                            'minimum': 0.0,
                            'maximum': 1.0
                        },
                        'k2o': {
                            'type': 'number',
                            'title': 'K2O',
                            'minimum': 0.0,
                            'maximum': 1.0
                        }
                    }
                }
            }
        }
    },
    'outputs': {
        'solwcad.out': {
            'title': 'Output result',
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
#            'minOccurs': 1,
#            'maxOccurs': 'unbounded',
#            'schema': {
            'type': 'array',
            'minItems': 1,
            'maxItems': 'unbounded',
            'contentMediaType': 'application/json',
            'items': {
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
    #         ".0118" , ".0232" , ".0378" , ".0306" ] } ] } }
}


class SolwcadProcessor_New(BaseRemoteExecutionProcessor):
    """Solwcad Processor example"""
    def __init__(self, processor_def):
        """
        Initialize object
        :param processor_def: provider definition

        :returns: pygeoapi.process.solwcad.SolwcadProcessor_New
        """
        super().__init__(processor_def, PROCESS_METADATA)

    def prepare_output(self, info, working_dir, outputs):
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
#                solwcad_out.append({"value": line_items.split()})
                solwcad_out.append(line_items.split())

#        output = {
#            'id': 'solwcad.out',
#            'value': solwcad_out
#        }
        output = solwcad_out

        return mimetype, output

    def prepare_input(self, data, working_dir, outputs):

        try:
            elaboration_type = data['elaboration_type']['value']
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
        return f'<SolwcadProcessor_New> {self.name}'
