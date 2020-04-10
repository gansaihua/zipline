import os
import argparse
from zipline.research import get_pricing


def worker(
    root_symbol,
    start,
    end,
    adjustment=None,
    roll_style='volume',
    offset=0,
    storage='.',
):

    df = get_pricing(
        'X' + root_symbol if len(root_symbol) == 1 else root_symbol,
        start_date=start,
        end_date=end,
        offset=offset,
        roll_style=roll_style,
        adjustment=adjustment,
    )

    df.to_csv(
        '{}/{}{}-{}-{}.csv'.format(storage,
                                   root_symbol,
                                   offset,
                                   roll_style,
                                   adjustment),
        index_label='Date Time',
        date_format='%Y-%m-%d',  # %H return single digit hour when export, bugs?
        header=['Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest'],
    )


def main():
    parser = argparse.ArgumentParser(
        description="Write continuous futures data to csv")

    parser.add_argument("--symbol", required=True,
                        help="root symbol for futures contract, e.g. `IF`, `XA`")
    parser.add_argument("--start", required=True,
                        help="start date for data fetching, e.g. 20200202")
    parser.add_argument("--end", required=True,
                        help="end date for data fetching, e.g. 20200204")
    parser.add_argument("--adjustment", required=False,
                        help="adjustment to price, use `add` or `mul`, default is None")
    parser.add_argument("--roll", required=False,
                        help="roll style, use `volume` or 'calendar', default is `volume`.")
    parser.add_argument("--offset", required=False,
                        help="the n-th contract followed by active contracts, default is 0")
    parser.add_argument("--storage", required=False,
                        help="The path were the files will be downloaded to, default is '.' ")

    args = parser.parse_args()

    storage = args.storage or '.'
    if not os.path.exists(storage):
        print("Creating {} directory".format(storage))
        os.mkdir(storage)

    worker(
        root_symbol=args.symbol,
        start=args.start,
        end=args.end,
        adjustment=args.adjustment,
        roll_style=args.roll or 'volume',
        offset=int(args.offset or 0),
        storage=storage,
    )


if __name__ == "__main__":
    main()
