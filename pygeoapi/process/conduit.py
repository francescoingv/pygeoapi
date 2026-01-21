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
import copy

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
    'id': 'conduit',
    # type string

    'version': '2.1.0',
    # type string

    # Altre proprietà non required:
    'jobControlOptions': [
        'async-execute',
        'sync-execute'
    ],
    # type: array,
    #   items: {type: string, enum: ['sync-execute', 'async-execute', 'dismiss']}
    
    #'proprieta_aggiunta': 'altra proprietà',
    
    # outputTransmission
    # type: array, 
    #   items: {type: string, enum: ['value', 'reference'], default: 'value'}
    'outputTransmission': [
        'value'
    ],

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
    'title': 'CONDUIT',
    # type: string

    'description':
        'Fortran code to calculate the multiphase, multicomponent '
        'steady flow of magma along the volcanic conduit.',
    # type: string
    
    'keywords': ['Fortran code', 'conduit flow', 'multiphase flow'],
    # type: array
    #   items: type: string

    # process.yaml
    'inputs': {
        # inputDescription.yaml
        'components': {
            'title': 'Model parameters', # opzionale
            'description':
                'Input parameters. Values must be in scientific '
                'notation. ', # opzionale
            # 'keywords' : [] # opzionale, array di stringhe
            # 'metadata' : ... # opzionale
            # additionalParameters # opzionale

            'minOccurs': 1, # opzionale
            'maxOccurs': 1, # opzionale
            'schema': { # mandatory
                'oneOf': [
                    {
                        'type': 'object',
                        'description':
                            'Parameters to calculate the multiphase '
                            '(melt+gas), multicomponent flow of '
                            'crystal-free magma along the volcanic conduit',
                        'required': [
                            'p', 't', 'd', 'l', 'sio2', 'tio2', 'al2o3',
                            'fe2o3', 'feo', 'mno', 'mgo', 'cao', 'na2o', 'k2o',
                            'h2o', 'co2', 'b'
                        ],
                        'properties': {
                            'f': {
                                'type': 'number',
                                'title': 'Initial guess for mass flow rate [kg/s]',
                                'description':
                                    'Initial guess for the numerical algorithm. '
                                    'Default value: 1.0E8. If the code does not '
                                    'converge, you can play around with this value.',
                                'exclusiveMinimum': 0.0
                            },
                            'p': {
                                'type': 'number',
                                'title': 'Pressure [Pa]',
                                'description':
                                    'Pressure in the magma chamber.',
                                'exclusiveMinimum': 0.0
                            },
                            't': {
                                'type': 'number',
                                'title': 'Temperature [K]',
                                'description':
                                    'Constant magma temperature',
                                'exclusiveMinimum': 0.0
                            },
                            'd': {
                                'type': 'number',
                                'title': 'Conduit diameter [m]',
                                'description':
                                    'Diameter of the cylindrical conduit',
                                'exclusiveMinimum': 0.0
                            },
                            'l': {
                                'type': 'number',
                                'title': 'Conduit length [m]',
                                'description':
                                    'Length of the cylindrical conduit',
                                'exclusiveMinimum': 0.0
                            },
                            'sio2': {
                                'type': 'number',
                                'title': 'SiO_2',
                                'description':
                                    'Melt composition: Weight fraction of SiO_2.',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'tio2': {
                                'type': 'number',
                                'title': 'TiO_2',
                                'description':
                                    'Melt composition: Weight fraction of TiO_2',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'al2o3': {
                                'type': 'number',
                                'title': 'Al_2O_3',
                                'description':
                                    'Melt composition: Weight fraction of Al_2O_3',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'fe2o3': {
                                'type': 'number',
                                'title': 'Fe_2O_3',
                                'description':
                                    'Melt composition: Weight fraction of Fe_2O_3',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'feo': {
                                'type': 'number',
                                'title': 'FeO',
                                'description':
                                    'Melt composition: Weight fraction of FeO',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'mno': {
                                'type': 'number',
                                'title': 'MnO',
                                'description':
                                    'Melt composition: Weight fraction of MnO',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'mgo': {
                                'type': 'number',
                                'title': 'MgO',
                                'description':
                                    'Melt composition: Weight fraction of MgO',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'cao': {
                                'type': 'number',
                                'title': 'CaO',
                                'description':
                                    'TBD: spiegazione',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'nao2': {
                                'type': 'number',
                                'title': 'TBD: significato',
                                'description':
                                    'TBD: spiegazione',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'k2o': {
                                'type': 'number',
                                'title': 'K_2O',
                                'description':
                                    'Melt composition: Weight fraction of K_2O',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'h2o': {
                                'type': 'number',
                                'title': 'H_2O',
                                'description':
                                    'Volatiles: Weight fraction of total H_2O',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'co2': {
                                'type': 'number',
                                'title': 'CO_2',
                                'description':
                                    'Volatiles: Weight fraction of total CO_2',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'b': {
                                'type': 'number',
                                'title': 'BND [m^{-3}]',
                                'description':
                                    'Bubble Number Density',
                                'exclusiveMinimum': 0.0
                            }
                        }
                    },
                    {
                        'type': 'object',
                        'description':
                            'Parameters to calculate the multiphase '
                            '(melt+crystals+gas), multicomponent flow of '
                            'magma along volcanic conduits.',
                        'required': [
                            'p', 't', 'd', 'l', 'sio2', 'tio2', 'al2o3',
                            'fe2o3', 'feo', 'mno', 'mgo', 'cao', 'na2o', 'k2o',
                            'h2o', 'co2', 'b', 'c', 'den'
                        ],
                        'properties': {
                            'f': {
                                'type': 'number',
                                'title': 'Initial guess for mass flow rate [kg/s]',
                                'description':
                                    'Initial guess for the numerical algorithm. '
                                    'Default value: 1.0E8. If the code does not '
                                    'converge, you can play around with this value.',
                                'exclusiveMinimum': 0.0
                            },
                            'p': {
                                'type': 'number',
                                'title': 'Pressure [Pa]',
                                'description':
                                    'Pressure in the magma chamber.',
                                'exclusiveMinimum': 0.0
                            },
                            't': {
                                'type': 'number',
                                'title': 'Temperature [K]',
                                'description':
                                    'Constant magma temperature',
                                'exclusiveMinimum': 0.0
                            },
                            'd': {
                                'type': 'number',
                                'title': 'Conduit diameter [m]',
                                'description':
                                    'Diameter of the cylindrical conduit',
                                'exclusiveMinimum': 0.0
                            },
                            'l': {
                                'type': 'number',
                                'title': 'Conduit length [m]',
                                'description':
                                    'Length of the cylindrical conduit',
                                'exclusiveMinimum': 0.0
                            },
                            'sio2': {
                                'type': 'number',
                                'title': 'SiO_2',
                                'description':
                                    'Melt composition: Weight fraction of SiO_2.',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'tio2': {
                                'type': 'number',
                                'title': 'TiO_2',
                                'description':
                                    'Melt composition: Weight fraction of TiO_2',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'al2o3': {
                                'type': 'number',
                                'title': 'Al_2O_3',
                                'description':
                                    'Melt composition: Weight fraction of Al_2O_3',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'fe2o3': {
                                'type': 'number',
                                'title': 'Fe_2O_3',
                                'description':
                                    'Melt composition: Weight fraction of Fe_2O_3',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'feo': {
                                'type': 'number',
                                'title': 'FeO',
                                'description':
                                    'Melt composition: Weight fraction of FeO',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'mno': {
                                'type': 'number',
                                'title': 'MnO',
                                'description':
                                    'Melt composition: Weight fraction of MnO',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'mgo': {
                                'type': 'number',
                                'title': 'MgO',
                                'description':
                                    'Melt composition: Weight fraction of MgO',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'cao': {
                                'type': 'number',
                                'title': 'CaO',
                                'description':
                                    'TBD: spiegazione',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'nao2': {
                                'type': 'number',
                                'title': 'TBD: significato',
                                'description':
                                    'TBD: spiegazione',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'k2o': {
                                'type': 'number',
                                'title': 'K_2O',
                                'description':
                                    'Melt composition: Weight fraction of K_2O',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'h2o': {
                                'type': 'number',
                                'title': 'H_2O',
                                'description':
                                    'Volatiles: Weight fraction of total H_2O',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'co2': {
                                'type': 'number',
                                'title': 'CO_2',
                                'description':
                                    'Volatiles: Weight fraction of total CO_2',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 1.0
                            },
                            'b': {
                                'type': 'number',
                                'title': 'BND [m^{-3}]',
                                'description':
                                    'Bubble Number Density',
                                'exclusiveMinimum': 0.0
                            },
                            'c': {
                                'type': 'number',
                                'title': 'Crystal volume fraction',
                                'description':
                                    'Volume fraction of crystals relative to a degassed magma.',
                                'exclusiveMinimum': 0.0,
                                'exclusiveMaximum': 0.7
                            },
                            'den': {
                                'type': 'number',
                                'title': 'Crystal density [kg/m^3]',
                                'description':
                                    'Average density of the crystal phase.',
                                'exclusiveMinimum': 0.0
                            }
                        }
                    },
                ]
            }
        }
    },
    'outputs': {
        'grafico_1': {
            'title': 'Output result',
            'description':
                'TBD: Colucci.',
#            'minOccurs': 1,
#            'maxOccurs': 'unbounded',
            'schema': {
                'contentMediaType': 'application/json', # da verificare che sia corretto .
                'type': 'object',
                'required': [
                    'Domain', 'Series'
                ],
                'properties': {
                    'Domain': {
                        'type': 'object',
                        'required': [
                            'label', 'unit', 'values'
                        ],
                        'properties': {
                            'label': {
                                'type': 'string'
                            },
                            'unit':  {
                                'type': 'string'
                            },
                            'values': {
                                'type': 'array',
                                'items': {
                                    'type': 'number'
                                }
                            }
                        }
                    },
                    'Series': {
                        'type': 'array',
                        'items': {
                            'type': 'object',
                            'required': [
                                'label', 'unit', 'values'
                            ],
                            'properties': {
                                'label': {
                                    'type': 'string'
                                },
                                'unit':  {
                                    'type': 'string'
                                },
                                'values': {
                                    'type': 'array',
                                    'items': {
                                        'type': 'number'
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        'grafico_2': {
            'title': 'Output result',
            'description':
                'TBD: Colucci.',
#            'minOccurs': 1,
#            'maxOccurs': 'unbounded',
            'schema': {
                'contentMediaType': 'application/json', # da verificare che sia corretto .
                'type': 'object',
                'required': [
                    'Domain', 'Series'
                ],
                'properties': {
                    'Domain': {
                        'type': 'object',
                        'required': [
                            'label', 'unit', 'values'
                        ],
                        'properties': {
                            'label': {
                                'type': 'string'
                            },
                            'unit':  {
                                'type': 'string'
                            },
                            'values': {
                                'type': 'array',
                                'items': {
                                    'type': 'number'
                                }
                            }
                        }
                    },
                    'Series': {
                        'type': 'array',
                        'items': {
                            'type': 'object',
                            'required': [
                                'label', 'unit', 'values'
                            ],
                            'properties': {
                                'label': {
                                    'type': 'string'
                                },
                                'unit':  {
                                    'type': 'string'
                                },
                                'values': {
                                    'type': 'array',
                                    'items': {
                                        'type': 'number'
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        'grafico_3': {
            'title': 'Output result',
            'description':
                'TBD: Colucci.',
#            'minOccurs': 1,
#            'maxOccurs': 'unbounded',
            'schema': {
                'contentMediaType': 'application/json', # da verificare che sia corretto .
                'type': 'object',
                'required': [
                    'Domain', 'Series'
                ],
                'properties': {
                    'Domain': {
                        'type': 'object',
                        'required': [
                            'label', 'unit', 'values'
                        ],
                        'properties': {
                            'label': {
                                'type': 'string'
                            },
                            'unit':  {
                                'type': 'string'
                            },
                            'values': {
                                'type': 'array',
                                'items': {
                                    'type': 'number'
                                }
                            }
                        }
                    },
                    'Series': {
                        'type': 'array',
                        'items': {
                            'type': 'object',
                            'required': [
                                'label', 'unit', 'values'
                            ],
                            'properties': {
                                'label': {
                                    'type': 'string'
                                },
                                'unit':  {
                                    'type': 'string'
                                },
                                'values': {
                                    'type': 'array',
                                    'items': {
                                        'type': 'number'
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        'csv': {
            'title': 'Dati in formato csv',
            'description': 'TBD',
            'schema': {
                'type': 'string',
                'contentMediaType': 'text/csv'
            }
        }
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
            'components': {
                'value': {'f': 1.0e8, 'p': 1.0e8, 't': 1050.0e0, 'd': 60.0e0,
                          'l': 4000.0e0, 'sio2': 0.7669, 'tio2': 0.0012, 
                          'al2o3': 0.1322, 'fe2o3': 0.0039, 'feo': 0.0038,
                          'mno': 0.0007, 'mgo': 0.0006, 'cao': 0.0080, 
                          'na2o': 0.0300, 'k2o': 0.0512, 'h2o': 0.0500e0,
                          'co2': 0.0200e0, 'b': 1.0e11, 'c': 0.2e0, 'den': 2800.0e0
                }
            }
        },
        'outputs': {
            'grafico_1': {
                'transmissionMode': 'value'
            },
            'grafico_2': {
                'transmissionMode': 'value'
            }
        }
    }
    # curl localhost:5000/processes/conduit/execution -H 'Content-Type: application/json' -d '{ "inputs" : { "components" : { "value" : {"f": 1.0e8, "p": 1.0e8, "t": 1050.0e0, "d": 60.0e0, "l": 4000.0e0, "sio2": 0.7669, "tio2": 0.0012, "al2o3": 0.1322, "fe2o3": 0.0039, "feo": 0.0038, "mno": 0.0007, "mgo": 0.0006, "cao": 0.0080, "na2o": 0.0300, "k2o": 0.0512, "h2o": 0.0500e0, "co2": 0.0200e0, "b": 1.0e11, "c": 0.2e0, "den": 2800.0e0 } } }, "outputs" : { "grafico_1" : { "transmissionMode": "value" }, "grafico_2" : { "transmissionMode": "value" } } }'
    # curl localhost:5000/processes/conduit/execution -H 'Content-Type: application/json' -H 'Prefer: respond-async' -d '{ "inputs" : { "components" : { "value" : {"f": 1.0e8, "p": 1.0e8, "t": 1050.0e0, "d": 60.0e0, "l": 4000.0e0, "sio2": 0.7669, "tio2": 0.0012, "al2o3": 0.1322, "fe2o3": 0.0039, "feo": 0.0038, "mno": 0.0007, "mgo": 0.0006, "cao": 0.0080, "na2o": 0.0300, "k2o": 0.0512, "h2o": 0.0500e0, "co2": 0.0200e0, "b": 1.0e11, "c": 0.2e0, "den": 2800.0e0 } } }, "outputs" : { "grafico_1" : { "transmissionMode": "value" }, "grafico_2" : { "transmissionMode": "value" } } }'
    # curl -k -L -X POST "https://epos_geoinquire.pi.ingv.it/epos_pygeoapi/processes/conduit/execution" -H "Content-Type: application/json" -d '{ "inputs" : { "components" : { "value" : {"f": 1.0e8, "p": 1.0e8, "t": 1050.0e0, "d": 60.0e0, "l": 4000.0e0, "sio2": 0.7669, "tio2": 0.0012, "al2o3": 0.1322, "fe2o3": 0.0039, "feo": 0.0038, "mno": 0.0007, "mgo": 0.0006, "cao": 0.0080, "na2o": 0.0300, "k2o": 0.0512, "h2o": 0.0500e0, "co2": 0.0200e0, "b": 1.0e11, "c": 0.2e0, "den": 2800.0e0 } } }, "outputs" : { "grafico_1" : { "transmissionMode": "value" }, "grafico_2" : { "transmissionMode": "value" } } }'
    #
    # ovvero:
    # curl localhost:5000/processes/conduit/execution 
    #       -H 'Content-Type: application/json' 
    #       -d '{ 
    #               "inputs" : { 
    #                   "components" : { 
    #                       "value" : {
    #                           "f": 1.0e8, "p": 1.0e8, "t": 1050.0e0, "d": 60.0e0, "l": 4000.0e0, 
    #                           "sio2": 0.7669, "tio2": 0.0012, "al2o3": 0.1322, "fe2o3": 0.0039, 
    #                           "feo": 0.0038, "mno": 0.0007, "mgo": 0.0006, "cao": 0.0080, "na2o": 0.0300, 
    #                           "k2o": 0.0512, "h2o": 0.0500e0, "co2": 0.0200e0, "b": 1.0e11, "c": 0.2e0, 
    #                           "den": 2800.0e0 
    #                       } 
    #                   } 
    #               },
    #               "outputs" : { 
    #                   "grafico_1" : { 
    #                       "transmissionMode": "value"
    #                   }, 
    #                   "grafico_2" : { 
    #                       "transmissionMode": "value" 
    #                   } 
    #               } 
    #           }'
}


class ConduitProcessor(BaseRemoteExecutionProcessor):
    """Conduit Processor example"""
    def __init__(self, processor_def):
        """
        Initialize object
        :param processor_def: provider definition

        :returns: pygeoapi.process.conduit.ConduitProcessor
        """
        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True


    def prepare_output(self, info, working_dir, outputs):
        # Only one output:
        #   "output in requested format"
        #   mediatype "as per output definition from process description"

        # Note: the 'code' may return a URL instead of the value.
        # but in this case the "product" is a string in URL format,
        # NOT the object/file that can be retrieved at the given URL.

        # Questo è il mimetype dell'intero risultato, non dei singoli
        # elementi del risultato che sono definiti nei metadati.
        mimetype = 'application/json' 

        # Il file di output non è passato come parametro ma è fisso e definito
        # all'interno del codice, e viene lasciato nella working dir

        # Carico il set di valori restituiti dal programma
        x_vals = []
        not_used = []
        gas_volume_fraction = []
        gas_velocity = []
        liquid_velocity = []
        pressure = []

        possible_outputs = self.metadata['outputs']

        if not bool(outputs):
            requested_outputs = possible_outputs
        else:
            requested_outputs = outputs

        out_file_name = 'duct.out'
        with open(str(Path(working_dir) / out_file_name), mode='r'
                  ) as output_file:
            for line in output_file:
                # Rimuovi spazi e usa split per separare i valori
                parts = line.split()
        
                # Converti 'D' in 'E' (notazione scientifica Python)
                values = [float(p.replace('D', 'E')) for p in parts]

                # Aggiungi i valori alle colonne
                x_vals.append(values[0])
                not_used.append(values[1])
                gas_volume_fraction.append(values[2])
                gas_velocity.append(values[3])
                liquid_velocity.append(values[4])
                pressure.append(values[5])

        # In funzione di quanto presente nel parametro outputs
        # predispongo gli elementi di output

        produced_outputs = {}
        if 'grafico_1' in requested_outputs:
            produced_outputs['grafico_1'] = {
                'value': {
                    'Domain': {
                        'label': 'Conduit length',
                        'unit': 'm',
                        'values': x_vals
                    },
                    'Series': [
                        {
                            'label': 'Gas volume fraction',
                            'unit': '',
                            'values': gas_volume_fraction
                        }
                    ]
                }
            }

        if 'grafico_2' in requested_outputs:
            produced_outputs['grafico_2'] = {
                'value': {
                    'Domain': {
                        'label': 'Conduit length',
                        'unit': 'm',
                        'values': x_vals
                    },
                    'Series': [
                        {
                            'label': 'Gas velocity',
                            'unit': 'm/s',
                            'values': gas_velocity
                        },
                        {
                            'label': 'Liquid velocity',
                            'unit': 'm/s',
                            'values': liquid_velocity
                        },
                    ]
                }
            }

        if 'grafico_3' in requested_outputs:
            produced_outputs['grafico_3'] = {
                'value': {
                    'Domain': {
                        'label': 'Conduit length',
                        'unit': 'm',
                        'values': x_vals
                    },
                    'Series': [
                        {
                            'label': 'Pressure',
                            'unit': 'Mpa',
                            'values': pressure
                        },
                    ]
                }
            }
        
        if 'csv' in requested_outputs:
            with open(str(Path(working_dir) / out_file_name), mode='r') as f:
                contenuto = f.read()

            produced_outputs['csv'] = {
                'value': contenuto,
                'mediaType': 'text/csv'
            }

        return mimetype, produced_outputs

    def prepare_input(self, data, working_dir, outputs):
        if bool(outputs):
            requested_output = set(outputs.keys() if isinstance(outputs, dict) else outputs)
            if requested_output - set(self.metadata['outputs']):
                err_msg = 'Outputs contains unexpected parameters.'
                raise ProcessorExecuteError(err_msg)

        try:
            components = data['components']['value']
        except Exception as err:
            err_msg = 'Input not correctly formatted: ' + str(err) + '\'.'
            raise ProcessorExecuteError(err_msg)
        
        # verifica che i parametri siano completi e non ce ne siano in eccesso.
        required_key_set = {'p', 't', 'd', 'l', 'sio2', 'tio2', 'al2o3',
                            'fe2o3', 'feo', 'mno', 'mgo', 'cao', 'na2o', 'k2o',
                            'h2o', 'co2', 'b'}
        optional_key_set = {'c', 'den'}
        default_key_set = {'f'}

        keys_present = set(components)
        if not required_key_set.issubset(keys_present):
            err_msg = 'Input does not contains all required parameters.'
            raise ProcessorExecuteError(err_msg)
        
        if optional_key_set.intersection(keys_present):  # Se almeno una opzionale è presente
            if not optional_key_set.issubset(keys_present):
                err_msg = 'Input does not contains all required parameters.'
                raise ProcessorExecuteError(err_msg)
            
        extra_keys = keys_present - required_key_set - optional_key_set - default_key_set
        if extra_keys:
            err_msg = f"Input contains unexpected parameters: {', '.join(extra_keys)}."
            raise ProcessorExecuteError(err_msg)

        # Create the dictionary with the properties to be passed to the 'code'
        # where property_name=parameter_name, property_value=parameter_value
        # ###############################################
        code_input_param = {}
        for name in components:
            # verifica che il formato dei numeri sia corretto
            param_value = components[name]
            if not isinstance(param_value, float):
                raise ProcessorExecuteError(f"Value 'components[{name}]' must be in decimal or scientific notation.")

            if name in ('f', 'p', 't', 'd', 'l', 'b', 'den'):
                if param_value <= 0.0:
                    raise ProcessorExecuteError(f"Value 'components[{name}]' must be > 0.0.")
            elif name in ('sio2', 'tio2', 'al2o3', 'fe2o3', 'feo', 'mno', 'mgo', 'cao', 'nao2', 'k2o', 'h20', 'co2'):
                if not (0.0 < param_value < 1.0):
                    raise ProcessorExecuteError(f"Value 'components[{name}]' must be >0.0 and <1.0.")
            elif name == 'c':
                if not (0.0 < param_value < 0.7):
                    raise ProcessorExecuteError(f"Value 'components[{name}]' must be >0.0 and <0.7.")

            input_flag = '-' + name
            code_input_param[input_flag] = param_value
        
        # si trattano i default_key_set
        if '-f' not in code_input_param:
            code_input_param['-f'] = 1.0e8

        return code_input_param

    def __repr__(self):
        return f'<ConduitProcessor> {self.name}'
