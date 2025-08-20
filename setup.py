import os
import setuptools

with open(os.path.join('jpart', 'resources', 'README.md')) as f:
    long_description = f.read()

_VERSION = '0.0.1'

_REQUIREMENTS = [
    'PyYAML',
    'ri-common >= 0.0.3',
]

setuptools.setup(
    name='jpart',
    version=_VERSION,
    description="Given sequential JSON data, partition and group the data based on rules",
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=[],
    author='Dustin Oprea',
    author_email='dustin@randomingenuity.com',
    packages=setuptools.find_packages(exclude=['tests']),
    include_package_data=True,
    zip_safe=False,
    package_data={
        'jpart': [
            'resources/README.md',
            'resources/requirements.txt',
            'resources/requirements-testing.txt',
        ],
    },
    install_requires=_REQUIREMENTS,
    scripts=[
        'jpart/resources/scripts/jpart',
    ],
)
