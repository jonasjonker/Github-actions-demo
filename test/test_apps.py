from pyspark.sql import Row
import pyspark.sql.types as T
import pytest

import apps.some_parser as parser


@pytest.fixture
def sdf(tmp_path):
    parser.setGlobalSparkSession("HelloWorld")
    file = tmp_path / "helloworld.txt"
    file.write_text("hello, world!")
    df = parser.readDataFrameFromText(str(file))
    return parser.verwerkDataFrame(df)


def test_some_parser_output(sdf):
    assert sdf.collect() == [Row(**{
        "value": "HELLO, WORLD!"
    })]


def test_some_parser_schema(sdf):
    assert sdf.schema == T.StructType([
        T.StructField("value", T.StringType(), True),
    ])
