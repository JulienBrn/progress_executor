from setuptools import setup, find_packages
from pathlib import Path


setup(
    name='progress_executor',
    packages=find_packages(where='src'),
    package_data={
        "progress_executor.package_data": ["*"],
    },
    entry_points={
        'console_scripts': [
            'progress_executor = progress_executor:run',
        ]
    },
    version='0.5',
    license='MIT',
    description = 'Subclasses of concurrent.future.Executor that correctly handle cancelling and progress',
    long_description = (Path(".") / "README.md").read_text(),
    long_description_content_type='text/markdown',
    author="Julien Braine",
    author_email='julienbraine@yahoo.fr',
    url='https://github.com/JulienBrn/progress_executor',
    download_url = 'https://github.com/JulienBrn/progress_executor.git',
    package_dir={'': 'src'},
    keywords=['python'],
    install_requires=["beautifullogger", "tqdm"],
    python_requires=">=3.11"
)
