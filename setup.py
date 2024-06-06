from setuptools import setup, find_packages

setup(
    name='dataclutch',  # パッケージ名
    version='0.0.1',  # バージョン番号
    author='tkykszk',  # 作者名
    author_email='tkykszk at gmail.com',  # 作者のメールアドレス
    description='A short description of the package',  # パッケージの短い説明
    long_description=open('README.md').read(),  # パッケージの長い説明
    long_description_content_type='text/markdown',  # 長い説明の内容タイプ
    url='https://github.com/xxxxxxxxx/xxxxxxx',  # パッケージのURL
    packages=find_packages(),  # パッケージを自動的に見つける
    classifiers=[
        'Programming Language :: Python :: 3',  # プログラミング言語
        'License :: OSI Approved :: MIT License',  # ライセンス
        'Operating System :: OS Independent',  # オペレーティングシステム
    ],
    python_requires='>=3.6',  # 必要なPythonのバージョン
    install_requires=[  # 依存パッケージ
    ],
    entry_points={  # コマンドラインツールのエントリーポイント
    },
)