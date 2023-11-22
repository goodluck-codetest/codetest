from setuptools import setup, find_packages

setup(
    name='quant1_codetest',
    version='0.1',
    packages=find_packages(),
    author='Simon Yau',
    author_email='shunmingyau@gmail.com',
    description='Code Test',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/YauShunMing/codetest/',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.8',
    ],
)