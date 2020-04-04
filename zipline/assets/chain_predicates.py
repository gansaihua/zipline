from functools import partial


def delivery_predicate(codes, contract):
    # This relies on symbols that are construct following a pattern of
    # root symbol + year + month, e.g. A1906, IF1909
    # This check would be more robust if the future contract class had
    # a 'delivery_month' member.
    delivery_code = contract.symbol.split('.', 2)[0][-2:]
    return delivery_code in codes


chain159 = partial(delivery_predicate, set(['01', '05', '09']))

CHAIN_PREDICATES = {
    # The majority of trading in CFEEX futures can be done in any month
    # we don't need skip contracts.


    # The majority of trading in these futures is done on a
    # June semiannual cycle (Jun, Dec) but contracts are
    # listed for the first 3 consecutive months from the present day. We
    # want the continuous futures to be composed of just the quarterly
    # contracts.
    'AG': partial(delivery_predicate, set(['06', '12'])),
    'AP': partial(delivery_predicate, set(['01', '05', '10'])),
    'AU': partial(delivery_predicate, set(['06', '12'])),

    'A': partial(delivery_predicate, set(['01', '05', '07', '09'])),
    'B': partial(delivery_predicate, set(['01', '03', '05', '07', '09', '11'])),
    'C': chain159,
    'CS': chain159,
    'EG': partial(delivery_predicate, set(['06', '09'])),
    'I': chain159,
    'J': chain159,
    'JD': chain159,
    'JM': chain159,
    'L': chain159,
    'M': chain159,
    'P': chain159,
    'PP': chain159,
    'V': chain159,
    'Y': chain159,
    'BU': partial(delivery_predicate, set(['03', '06', '09', '12'])),
}