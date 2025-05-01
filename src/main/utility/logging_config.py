from loguru import logger
import sys

logger.remove(0)
logger.add(sys.stderr, format="<green>{time}</green> | {level} | {message}")


# When you import a Python module (logging_config.py in this case), Python executes all the top-level code in that file once (when it’s first imported).

# That means:

# Variable assignments

# Class definitions

# Function definitions

# Function calls (if they're not inside if __name__ == "__main__":)

# All of those will be executed when the module is imported.

# If logging_config is imported in multiple places, Python runs it only the first time (thanks to module caching).
# After that, it reuses the module from memory.

# Want to avoid python calling all the lines in a imported file, wrap the lines that you dont want to run using below method.

if __name__ == "__main__":
    logger.info("Hello World!")
