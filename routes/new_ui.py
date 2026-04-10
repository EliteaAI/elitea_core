import json
import flask
from pathlib import Path

from pylon.core.tools import web, log

from werkzeug.exceptions import NotFound


class Route:
    @web.route('/access_denied', endpoint='route_access_denied')
    def access_denied_page(self):
        """ Styled access denied page """
        return self.descriptor.render_template("access_denied.html"), 403

    @web.route('/', defaults={'sub_path': ''}, endpoint='route_elitea_ui')
    @web.route('/<path:sub_path>', endpoint='route_elitea_ui_sub_path')
    def elitea_ui_react(self, sub_path: str):
        base_path = self.elitea_base_path
        try:
            return self.bp.send_static_file(base_path.joinpath(sub_path))
        except NotFound:
            log.info(
                "Route route_elitea_ui_sub_path: %s; serving: %s",
                sub_path, base_path.joinpath('index.html')
            )
            #
            elitea_ui_config_data = self.get_elitea_ui_config()
            vite_base_uri = elitea_ui_config_data["vite_base_uri"]
            elitea_ui_config = json.dumps(elitea_ui_config_data)
            #
            idx_path = Path(self.bp.static_folder).joinpath(base_path, "index.html")
            #
            with open(idx_path, "r", encoding="utf-8") as idx_file:
                idx_data = idx_file.read()
            #
            idx_data = idx_data.replace(
                'src="./assets', f'src="{vite_base_uri}/assets'
            )
            idx_data = idx_data.replace(
                'href="./assets', f'href="{vite_base_uri}/assets'
            )
            idx_data = idx_data.replace(
                '<!-- elitea_ui_config -->',
                f"<script>window.elitea_ui_config = JSON.parse('{elitea_ui_config}');</script>"
            )
            #
            response = flask.make_response(idx_data, 200)
            return response
