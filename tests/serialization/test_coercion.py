import numpy as np

from decs.coercion import coerce_argument_types


def test_coercion():
    @coerce_argument_types
    def test_func(a: int, b: float, c: str, d: bool, f: np.ndarray, g: np.ndarray[np.float32]):
        assert isinstance(a, int)
        assert isinstance(b, float)
        assert isinstance(c, str)
        assert isinstance(d, bool)
        assert isinstance(f, np.ndarray)
        assert isinstance(g, np.ndarray)
        assert g.dtype == np.float32

        return a, b, c, d, f, g

    t_a, t_b, t_c, t_d, t_f, t_g = test_func(1.6, 2, 5, 0, [1, 2, 3], [1, 2, 3])
    assert t_a == 1
    assert t_b == 2.0
    assert t_c == "5"
    assert isinstance(t_d, bool) and t_d is False
    assert np.array_equal(t_f, np.array([1, 2, 3]))
    assert np.array_equal(t_g, np.array([1, 2, 3], dtype=np.float32))
