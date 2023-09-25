#!/usr/bin/env python3

import argparse
import logging
from pathlib import Path

import prsv_tools.utility.cli as prsvcli

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)