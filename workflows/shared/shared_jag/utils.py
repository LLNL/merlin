try:
    # Important to use cStringIO, not StringIO,
    # as it is friendlier with the JSON
    from cStringIO import StringIO
except ImportError:
    from io import StringIO


def pack(obj):
    """
    TODO
    """
    pfile = StringIO()
    pickle.dump(obj, pfile)
    to_return = pfile.getvalue()
    pfile.close()
    return to_return
