##
## This Makefile only exists in order to build the figures for the spec.
##


PACKAGE_NAME = IonJava
BRAZIL_PACKAGE_VERSION?=1.0
PACKAGE_SUBDIRECTORY = shared/platform/IonJava


png_dir := build/brazil-documentation/figures
figure_names := $(basename $(notdir $(wildcard doc/figures/*.ms)))
png_names := $(patsubst %,$(png_dir)/%.png,$(figure_names))

figures: $(png_dir) $(png_names)

$(png_dir):
	mkdir -p $@

$(png_dir)/%.png: doc/figures/%.ms
	bin/dformat $< | \
	    groff -p -P-pletter -Tps | \
	    gs -q -sDEVICE=ppm -sOutputFile=- -r300x300 - | \
	    pnmcrop | \
	    pnmscale 0.33 | \
	    pnmtopng > $@
