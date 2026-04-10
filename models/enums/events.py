from enum import Enum


class ApplicationEvents(str, Enum):
    application_deleted = 'application_deleted'
    application_updated = 'application_updated'
    application_version_deleted = 'application_version_deleted'
    toolkit_updated = 'toolkit_updated'
    toolkit_deleted = 'toolkit_deleted'
