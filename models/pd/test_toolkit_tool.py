#!/usr/bin/python3
# coding=utf-8

#   Copyright 2024 EPAM Systems
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

""" Pydantic models for testing toolkit tools """
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field


class TestToolkitToolInputModel(BaseModel):
    """Input model for testing toolkit tools"""
    
    toolkit_config: Dict[str, Any] = Field(
        ..., 
        description="Configuration for the toolkit containing toolkit_name and settings"
    )
    tool_name: str = Field(..., description="Name of the tool to test")
    tool_params: Optional[Dict[str, Any]] = Field(
        default_factory=dict, 
        description="Parameters to pass to the tool"
    )
    
    # LLM Configuration (simplified - elitea-sdk will handle the rest)
    llm_model: Optional[str] = Field(
        default="gpt-4o-mini", 
        description="LLM model name to use for testing"
    )
    llm_settings: Optional[Dict[str, Any]] = Field(
        default_factory=lambda: {
            "max_tokens": 1024,
            "temperature": 0.1
        },
        description="Basic LLM settings (temperature, max_tokens, etc.)"
    )
    
    # Communication
    sid: Optional[str] = Field(None, description="Socket ID for real-time communication")
    project_id: Optional[int] = Field(None, description="Project ID (automatically added from URL parameter)")
    
    # Event tracking
    stream_id: Optional[str] = Field(None, description="Stream ID for event tracking")
    message_id: Optional[str] = Field(None, description="Message ID for event tracking")

    class Config:
        extra = "allow"  # Allow additional fields


class TestToolkitToolResponseModel(BaseModel):
    """Response model for testing toolkit tools"""
    
    success: bool = Field(..., description="Whether the test was successful")
    result: Optional[Any] = Field(None, description="Result from the tool if successful")
    error: Optional[str] = Field(None, description="Error message if unsuccessful")
    tool_name: str = Field(..., description="Name of the tested tool")
    toolkit_config: Dict[str, Any] = Field(..., description="Original toolkit configuration")
    llm_model: Optional[str] = Field(None, description="LLM model used for the test")
    events_dispatched: Optional[list] = Field(
        default_factory=list, 
        description="List of custom events dispatched during execution"
    )
    execution_time_seconds: Optional[float] = Field(
        None, 
        description="Time taken to execute the tool in seconds"
    )
    
    # Task information
    task_id: Optional[str] = Field(None, description="Task ID if running asynchronously")
    stream_id: Optional[str] = Field(None, description="Stream ID for event tracking")
    message_id: Optional[str] = Field(None, description="Message ID for event tracking")

    class Config:
        extra = "allow"  # Allow additional fields
