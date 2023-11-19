from setuptools import setup, find_packages

setup(
    name='showroom',
    version='0.4.0',
    packages=['showroom',],
    license='MIT',
    python_requires=">=3.6, <4",
    long_description=open('README.md').read(),
    entry_points={
        "console_scripts": ["showroom=showroom.main:main", "archiver=showroom.archive.main:main"]
    },
    # TODO: determine minimum/maximum required versions if any
    install_requires=[
        "requests",
        "pyyaml",
        "m3u8",  # why does requirements.txt require >=0.5.4
        "websocket-client",
    ]
)