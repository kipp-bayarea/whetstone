from distutils.core import setup

setup(
    name='whetstone',
    version='1.1',
    description='KIPP DC Whetstone Library',
    packages=['whetstone'],
    install_requires=["requests", "json","os"],
    author="Melvin Freeman",
    author_email="melvin.freeman@kippdc.org",
    include_package_data=True,
    long_description=open('README.md').read(),
    zip_safe=False
)
