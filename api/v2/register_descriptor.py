#!/usr/bin/python3
# coding=utf-8
# pylint: disable=R0912,R0914

#   Copyright 2025 EPAM Systems
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

""" API """

import json

import flask  # pylint: disable=E0401
import flask_restful  # pylint: disable=E0401

from pydantic import ValidationError  # pylint: disable=E0401

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from tools import this, auth, config as c  # pylint: disable=E0401

from ...db.models.providers import ProviderDescriptor


class API(flask_restful.Resource):  # pylint: disable=R0903
    """ API """

    url_params = [
        "<int:project_id>",
    ]

    def __init__(self, module):
        self.module = module

    @auth.decorators.check_api(
        permissions={
            "permissions": ["provider_hub.descriptor.register"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "viewer": False, "editor": False},
            },
        },
        mode=c.ADMINISTRATION_MODE,
    )
    def post(self, project_id):
        """ Process POST """
        try:
            descriptor_data = flask.request.data
            descriptor = self.module.descriptor_model.model_validate_json(descriptor_data)
            #
            descriptor_key = json.dumps(
                {
                    "project_id": project_id,
                    "provider_name": descriptor.name,
                    "service_location_url": str(descriptor.service_location_url),
                },
                sort_keys=True,
            )
            #
            # Save / update in DB
            #
            with this.db.session as session:
                descriptor_obj = session.query(ProviderDescriptor).get(descriptor_key)
                #
                if descriptor_obj is None:
                    descriptor_obj = ProviderDescriptor(
                        provider=descriptor_key,
                        descriptor=descriptor_data,
                    )
                    #
                    session.add(descriptor_obj)
                else:
                    descriptor_obj.descriptor = descriptor_data
            #
            # Init
            #
            self.module.init_provider(project_id, descriptor)
            #
            return {"ok": True}
        except ValidationError as exc:
            return {"ok": False, "errors": exc.errors()}
        except:  # pylint: disable=W0702
            log.exception("Error during provider registration")
            #
            return {"ok": False}

    @auth.decorators.check_api(
        permissions={
            "permissions": ["provider_hub.descriptor.register"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "viewer": False, "editor": False},
            },
        },
        mode=c.ADMINISTRATION_MODE,
    )
    def delete(self, project_id):
        """ Process DELETE """
        provider_name = flask.request.args.get("provider_name")
        service_location_url = flask.request.args.get("service_location_url")
        #
        descriptor_key = json.dumps(
            {
                "project_id": project_id,
                "provider_name": provider_name,
                "service_location_url": service_location_url,
            },
            sort_keys=True,
        )
        #
        self.module.deinit_provider(project_id, provider_name, service_location_url)
        #
        with this.db.session as session:
            descriptor_obj = session.query(ProviderDescriptor).get(descriptor_key)
            #
            if descriptor_obj is not None:
                session.delete(descriptor_obj)
        #
        return {"ok": True}
