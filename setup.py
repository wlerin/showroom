from setuptools import setup

deps = [
    "requests",
    "fake-useragent",
    "pyyaml",
    "websocket-client"
]

setup(
    name='showroom',
    version='0.3.5-master',
    packages=['showroom'],
    license='MIT',
    long_description=open('README.md').read(),
    install_requires=deps,
    python_requires=">=3.6, <4",
    entry_points={
        "console_scripts": [
            "showroom=showroom.main:main",
            "archiver=showroom.archive.main:main",
            # TODO: merge concat into archiver
            "concat=concat",
        ]
    }
)
