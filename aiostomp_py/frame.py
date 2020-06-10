from typing import Dict, Union, Optional


class Frame:
    def __init__(
        self, command: str, headers: Optional[Dict[str, str]] = None, body: Optional[Union[str, bytes, None]] = None
    ):
        self.command = command
        if "\n" in self.command:
            raise RuntimeError(f"Invalid command {self.command}")
        self.headers = headers
        self.body = body

    def __repr__(self) -> str:
        headers = ""
        if self.headers:
            headers = ";".join(f"{key}: {value}" for key, value in self.headers.items())
        return f"<Frame: {self.command} headers: {headers}> body: {self.body}"
