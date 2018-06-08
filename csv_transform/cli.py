import csv
import json
import typing
import click
from csv_transform.mapper import CSVMapper


@click.command()
@click.argument('mapfile', type=click.File('r', encoding='utf-8-sig'))
@click.argument('input', type=click.Path(exists=True))
@click.argument('encoding', type=str, default='utf-8-sig')
def map(mapfile: typing.IO[str], input: str, encoding: str) -> None:
    with open(input, 'r', newline='', encoding=encoding) as file:
        mapper = CSVMapper.from_mapfile(json.load(mapfile))
        reader = csv.reader(file)
        next(reader)
        for row in mapper.apply(reader):
            print(row)
