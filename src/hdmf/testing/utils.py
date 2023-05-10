import os


def remove_test_file(path):
    """A helper function for removing intermediate test files

    This checks if the environment variable CLEAN_HDMF has been set to False
    before removing the file. If CLEAN_HDMF is set to False, it does not remove the file.
    """
    false_options = (
        "False",
        "false",
        "FALSE",
        "0",
        0,
        False,
    )
    clean_flag_set = os.getenv("CLEAN_HDMF", True) not in false_options
    if os.path.exists(path) and clean_flag_set:
        os.remove(path)
