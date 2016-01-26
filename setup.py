from setuptools import setup

tests_require = ['testtools']

setup(
    name='SpreadFlowDelta',
    version='0.0.1',
    description='Common SpreadFlow processors for delta-type messages',
    author='Lorenz Schori',
    author_email='lo@znerol.ch',
    url='https://github.com/znerol/spreadflow-delta',
    packages=[
        'spreadflow_delta',
        'spreadflow_delta.test'
    ],
    install_requires=[
        'SpreadFlowCore'
    ],
    tests_require=tests_require,
    extras_require={
        'tests': tests_require
    },
    zip_safe=False,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Framework :: Twisted',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Topic :: Multimedia'
    ]
)
