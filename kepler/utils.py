def _convert_object_from_cassandra(t, values):
    if t == 'str':
        return values[1]
    elif t == 'scalar':
        return values[0]
    elif t == 'numpy':
        out = io.BytesIO(values[2])
        out.seek(0)
        return np.load(out)