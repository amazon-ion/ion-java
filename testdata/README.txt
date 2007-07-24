This folder contains samples of Ion content for use by compatability test
suites.

The content is partitioned as follows:

  * bad
      All files in this directory are invalid Ion and should fail parsing.
      Most files should include comments indicating the problem.
  * good
      All files in this directory are valid Ion.
  * equivs
      Each file in this directory consists of a sequence of Ion values, all of
      which should be equivalent.
