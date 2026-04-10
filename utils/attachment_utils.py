#
#   Copyright 2026 EPAM Systems
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

import base64
from typing import Optional

from pylon.core.tools import log


def extract_as_bytes_from_base64_content(content) -> Optional[bytes]:
    """
    Extract and decode base64 content from vision message format to bytes.
    
    Handles both single message and list of message chunks.
    Standard OpenAI vision format:
    {
        "type": "image_url",
        "image_url": {
            "url": "data:image/png;base64,iVBORw0KGgoAAAANS..."
        }
    }
    
    Args:
        content: Vision message dict or list of dicts with image_url.url
        
    Returns:
        Decoded base64 bytes or None if extraction fails
    """
    if not content:
        return None
    
    try:
        # Handle list of message chunks (multiple vision messages)
        if isinstance(content, list):
            for chunk in content:
                if isinstance(chunk, dict):
                    url = chunk.get('image_url', {}).get('url', '')
                    if url.startswith('data:'):
                        base64_data = url.split(',', 1)[1] if ',' in url else None
                        if base64_data:
                            return base64.b64decode(base64_data)
        
        # Handle single vision message dict
        elif isinstance(content, dict):
            url = content.get('image_url', {}).get('url', '')
            if url.startswith('data:'):
                base64_data = url.split(',', 1)[1] if ',' in url else None
                if base64_data:
                    return base64.b64decode(base64_data)
        
        return None
        
    except Exception as e:
        log.warning(f"Failed to extract base64 from content: {e}")
        return None
