if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


# Runs right after load_iris: inspects the raw table before anything uses it.
# It passes the table on unchanged, so prepare_data still receives 150 rows.
@transformer
def check(df, *args, **kwargs):
    missing = int(df.isna().sum().sum())
    classes = sorted(df['target'].unique().tolist())
    print('Missing values:', missing, '| Classes:', classes)
    return df


@test
def test_no_missing_and_three_classes(output, *args):
    assert output.isna().sum().sum() == 0, 'the data has missing values'
    assert output['target'].nunique() == 3, 'expected exactly 3 classes'
