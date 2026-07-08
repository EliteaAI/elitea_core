import flask

from pylon.core.tools import web, log


class Route:
    @web.route("/skill_icon/<path:sub_path>")
    def skill_icon(self, sub_path):
        return flask.send_from_directory(self.skill_icon_path, sub_path)
