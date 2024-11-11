"""setup."""
import os
import setuptools
try:  # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError:  # for pip <= 9.0.3
    from pip.req import parse_requirements


with open(os.path.join(os.path.dirname(__file__), 'README.md')) as readme:
    README = readme.read()

requirements_path = os.path.join(
    os.path.dirname(__file__), 'requirements.txt')
install_reqs = parse_requirements(requirements_path, session=False)
try:
    requirements = [str(ir.req) for ir in install_reqs]
except Exception:
    requirements = [str(ir.requirement) for ir in install_reqs]


# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setuptools.setup(
    name='pumpwood-miscellaneous',
    version='0.26.0',
    include_package_data=True,
    license='BSD-3-Clause License',
    description='Miscellaneous class and funcitions used in Pumpwood.',
    long_description=README,
    long_description_content_type="text/markdown",
    url='https://github.com/Murabei-OpenSource-Codes/pumpwood-miscellaneous',
    author='Murabei Data Science',
    author_email='a.baceti@murabei.com',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    install_requires=[
        "python-slugify>=6.1.1",
        "pandas>=1.0",
        "boto3==1.26.101",
        "google-cloud-storage==1.28.1",
        "azure-storage-blob==12.12.0",
        "markupsafe==2.0.1",
        "Werkzeug==1.0.1",
        "pika==0.12",
        "Flask-SQLAlchemy==2.3.2",
        "GeoAlchemy2>=0.9.3",
        "Flask>=1.1.4",
    ],
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
)
