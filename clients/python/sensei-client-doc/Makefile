EPYDOC=epydoc
DSTDOC=docs 

doc: clean-doc 
	$(EPYDOC) --html --graph=all -v -o $(DSTDOC) ../sensei/sensei_client_lib.py  \
	../sensei/sensei_client_lib_test.py 

clean-doc:  
	rm -rf $(DSTDOC)
