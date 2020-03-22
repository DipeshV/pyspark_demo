"""Setup file
Tutorial:
  http://the-hitchhikers-guide-to-packaging.readthedocs.io/en/latest/quickstart.html

rm -rf dist/
python setup.py sdist bdist_wheel
cd ..
pip install -I pyspark_demo/dist/pyspark_demo-*.whl  # Must be outside the project root
cd pyspark_demo

"""
import setuptools  # this is for bdist wheel


setuptools.setup(
    name="pyspark_demo",
    version=1.,
    author_email="punchh.com",
    url="",
    packages=setuptools.find_packages(),
    package_dir={
        "pyspark_demo": "pyspark_demo",
        "pyspark_demo.apps": "pyspark_demo/apps",
        "pyspark_demo.commons": "pyspark_demo/commons"
    },
    python_requires=">=3.7",
    license=""
)
