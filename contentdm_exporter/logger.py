# Copyright (C) 2024 TIND.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import logging


def setup_logger(logger_file, logger_name):

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    file_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d  %H:%M:%S"
    )
    stream_formatter = logging.Formatter("%(levelname)s - %(message)s")

    file_handler = logging.FileHandler(logger_file, "w")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.CRITICAL)
    stream_handler.setFormatter(stream_formatter)
    logger.addHandler(stream_handler)

    logger.info("Logger %r initialized!", logger_name)
