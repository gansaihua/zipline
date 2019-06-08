from zipline.pipeline.factors import CustomFactor


class LatestEconomics(CustomFactor):
    '''
    To use the single value datasets (ie ones that don't have a value for each asset)
    one needs to ceate a simple custom factor

    pipe4 = Pipeline(
        columns={
            'pmi_manufacturing': LatestMacroEconomic(inputs=[MacroEconomic.pmi_manufacturing]),
            'pmi_manufacturing_asof': LatestMacroEconomic(inputs=[MacroEconomic.pmi_manufacturing_asof],
                                                            dtype=np.dtype('datetime64[ns]')),
        },
        domain=CN_EQUITIES,
    )
    '''
    window_length = 1

    def compute(self, today, asset_ids, out, values):
        out[:] = values[-1]