import re
import json
import importlib.util
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable

from benchmark.basic import client
from benchmark.basic.my_logger import logger


class Task:
    """Represents a single task in a workload."""
    
    def __init__(self, name: str, config: Dict[str, Any], global_params: Dict[str, Any], workload_dir: Path):
        self.name = name
        self.config = config
        self.workload_dir = workload_dir
        # Merge global params with task-specific params (task params override global)
        self.parameters = {**global_params, **(config.get('parameters') or {})}
        
    def _substitute_params(self, content: str) -> str:
        """Replace {{param}} placeholders with actual values."""
        def replacer(match):
            key = match.group(1)
            if key in self.parameters:
                return str(self.parameters[key])
            logger.warning(f"Parameter '{key}' not found, keeping placeholder")
            return match.group(0)
        
        return re.sub(r'\{\{(\S+?)\}\}', replacer, content)
    
    def _load_payload(self) -> Optional[Dict[str, Any]]:
        """Load and process JSON payload file."""
        payload_file = self.config.get('payload')
        if not payload_file:
            return None
            
        payload_path = self.workload_dir / payload_file
        if not payload_path.exists():
            raise FileNotFoundError(f"Payload file not found: {payload_path}")
            
        with open(payload_path, 'r') as f:
            content = f.read()
        
        # Substitute parameters
        content = self._substitute_params(content)
        return json.loads(content)
    
    def _load_script(self) -> Optional[Callable]:
        """Load Python script and return the doc_generator function."""
        script_file = self.config.get('script')
        if not script_file:
            return None

        script_path = self.workload_dir / script_file
        if not script_path.exists():
            raise FileNotFoundError(f"Script file not found: {script_path}")

        # Dynamically load the module
        spec = importlib.util.spec_from_file_location("workload_script", script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Get doc_generator function from the module
        if not hasattr(module, 'doc_generator'):
            raise ValueError(f"Script '{script_file}' must define a 'doc_generator' function")

        return module.doc_generator

    def execute(self) -> Any:
        """Execute the task. Override in subclasses for specific task types."""
        raise NotImplementedError(f"Task type '{self.name}' not implemented")


