all: DATA.stamp


clean:
	git clean -xdf .

DATA.stamp: plot_usage_history.py gen_all.sh derecho.yaml
	$(MAKE) clean
	./gen_all.sh
	./plot_usage_history.py ./derecho.yaml
	$(MAKE) link_plots
	date > $@


link_plots:
	for linkfile in $$(find . -type l -name "*.dat"); do \
	  truefile=$$(readlink $${linkfile}) ;\
	  for suffix in png pdf; do \
	    trueimg=$$(basename $${truefile}).$${suffix} ;\
	    linkimg=$$(basename $${linkfile}).$${suffix} ;\
	    if [[ -f $${trueimg} ]]; then \
	      ln -sf $${trueimg} $${linkimg} ;\
	      ls -l $${linkimg} ;\
	    fi ;\
	  done ;\
	done

%.py : %.ipynb Makefile
	jupyter nbconvert --clear-output $<
	jupyter nbconvert --to=python $< >/dev/null
	git add $@ $<
	chmod +x $@
