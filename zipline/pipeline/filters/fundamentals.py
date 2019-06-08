from zipline.pipeline.data.fundamentals import Fundamentals


def CSI300():
    return Fundamentals.memb_csi300.latest.eq(1)

def CSI500():
    return Fundamentals.memb_csi500.latest.eq(1)
