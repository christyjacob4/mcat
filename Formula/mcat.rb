class Mcat < Formula
  include Language::Python::Virtualenv

  desc "cat on steroids — Parquet, Avro, ORC, CSV, JSONL and remote sources"
  homepage "https://christyjacob4.github.io/mcat"
  url "https://github.com/christyjacob4/mcat/archive/refs/tags/v0.1.0.tar.gz"
  # sha256 is filled automatically at release time by the release workflow
  sha256 "PLACEHOLDER"
  license "MIT"

  depends_on "python@3.12"

  # Resource sha256 values are filled at release time.
  # Run `brew audit --new-formula mcat` after publishing to validate.

  resource "typer" do
    url "https://files.pythonhosted.org/packages/typer/typer-0.9.0.tar.gz"
    sha256 "PLACEHOLDER"
  end

  resource "rich" do
    url "https://files.pythonhosted.org/packages/rich/rich-13.0.0.tar.gz"
    sha256 "PLACEHOLDER"
  end

  resource "pyarrow" do
    url "https://files.pythonhosted.org/packages/pyarrow/pyarrow-14.0.0.tar.gz"
    sha256 "PLACEHOLDER"
  end

  resource "fsspec" do
    url "https://files.pythonhosted.org/packages/fsspec/fsspec-2024.1.0.tar.gz"
    sha256 "PLACEHOLDER"
  end

  def install
    virtualenv_install_with_resources
  end

  test do
    assert_match "mcat", shell_output("#{bin}/mcat --version")
    assert_match "hello", shell_output("echo hello | #{bin}/mcat")
  end
end
