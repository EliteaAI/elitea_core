import requests
from datetime import datetime

from flask import g, request, jsonify
from pylon.core.tools import log

from tools import api_tools, auth, config as c


class AdminAPI(api_tools.APIModeHandler):
    """ API """

    # @auth.decorators.check_api({
    #     "permissions": ["models.prompts.ui.detail"],
    #     "recommended_roles": {
    #         c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
    #     }})
    def get(self, **kwargs):
        """ GET """
        _ = kwargs
        #
        try:
            build_meta = self.module.get_build_meta()
            return jsonify(build_meta)
        except:  # pylint: disable=W0702
            log.exception("Failed to get build meta")
        #
        return {}

    @auth.decorators.check_api({
        "permissions": ["models.prompts.ui.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
        }})
    def post(self, **kwargs):
        """ POST """
        _ = kwargs
        #
        try:
            release = request.json.get('release', None)
            log.info("Downloading and installing release: %s", release)
            #
            self.module.update_ui(release)
            self.module.update_build_meta({
                "release": release,
                "updated_at": datetime.now(),
                "commit_sha": request.json.get('commit_sha', ''),
                "commit_ref": request.json.get('commit_ref', ''),
            })
            #
            build_meta = self.module.get_build_meta()
            return jsonify(build_meta)
        except:  # pylint: disable=W0702
            log.exception("Failed to update UI")
        #
        return {}


class API(api_tools.APIBase):  # pylint: disable=R0903
    """ API """
    url_params = ['', '<string:mode>']
    mode_handlers = {
        c.ADMINISTRATION_MODE: AdminAPI,
    }
