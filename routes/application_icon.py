import flask

from pylon.core.tools import web, log


class Route:
    @web.route("/application_icon/<path:sub_path>")
    def application_icon(self, sub_path):
        return flask.send_from_directory(self.application_icon_path, sub_path)

    @web.route("/application_tool_icon/<path:sub_path>")
    def application_tool_icon(self, sub_path):
        return flask.send_from_directory(self.application_tool_icon_path, sub_path)

    @web.route("/default_entity_icons/<path:sub_path>")
    def default_entity_icons(self, sub_path):
        return flask.send_from_directory(self.default_entity_icons_path, sub_path)
