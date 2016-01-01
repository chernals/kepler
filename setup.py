from setuptools import setup

setup(
    name='varilog',
    version='0.1',
    py_modules=['vrl'],
    install_requires=[
        'Click',
    ],
    entry_points='''
        [console_scripts]
        vrl=vrl:cli
    ''',
)