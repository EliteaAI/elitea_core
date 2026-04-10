#!/usr/bin/python3
# coding=utf-8

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

""" Provider descriptor DB model """

from sqlalchemy import Column, Text, LargeBinary  # pylint: disable=E0401

from tools import this  # pylint: disable=E0401


class ProviderDescriptor(this.db.Base):  # pylint: disable=C0111,R0903
    __tablename__ = f"{this.module_name}_descriptor"

    provider = Column(Text, primary_key=True)
    descriptor = Column(LargeBinary, unique=False, default=b"")
