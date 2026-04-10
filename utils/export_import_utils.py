ENTITY_IMPORT_MAPPER = {
    'datasources': 'datasources_import_datasource',
    'agents': 'applications_import_application',
    'toolkits': 'applications_import_toolkit',
}

def _wrap_import_error(ind, msg):
    return {
        "index": ind,
        "msg": msg
    }


def _wrap_import_result(ind, result):
    result['index'] = ind
    return result