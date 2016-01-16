from setuptools import setup

setup(
    name='kepler',
    version='0.2',
    py_modules=['kep'],
    install_requires=[
        'Click',
    ],
    entry_points='''
        [console_scripts]
        kep=kep:cli
    ''',
)