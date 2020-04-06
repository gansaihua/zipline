def ensure_list(x):
    if isinstance(x, str):
        x = [x]

    try:
        x = list(iter(x))
    except TypeError:
        x = [x]

    return x


def _invert(d):
    return dict(zip(d.values(), d.keys()))
