from arti.types.core import Float16, Float32, Float64, Int32, Int64, String


def test_python_TypeSystem() -> None:
    from arti.types.python import python

    assert isinstance(python.to_artigraph(int), Int64)
    for int_type in (Int64, Int32):
        assert python.to_system(int_type()) is int

    assert isinstance(python.to_artigraph(float), Float64)
    for float_type in (Float64, Float32, Float16):
        assert python.to_system(float_type()) is float

    assert isinstance(python.to_artigraph(str), String)
    assert python.to_system(String()) is str
