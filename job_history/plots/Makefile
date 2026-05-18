all: DATA.stamp


clean:
	git clean -xdf .

DATA.stamp: plot_usage_history.py gen_all.sh derecho.yaml
	$(MAKE) clean
	./gen_all.sh
	date > $@

%.py : %.ipynb Makefile
	jupyter nbconvert --clear-output $<
	jupyter nbconvert --to=python $< >/dev/null
	git add $@ $<
	chmod +x $@
