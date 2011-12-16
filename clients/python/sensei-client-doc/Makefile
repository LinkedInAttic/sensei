EPYDOC=epydoc
DSTDOC=docs 

doc: clean-doc 
	$(EPYDOC) --html --graph=all -v -o $(DSTDOC) ../sensei/sensei_client_lib.py \
        ../sensei/bql_parser.py ../sensei/sensei_client.py ../sensei/sensei_client_lib_test.py \
        ../sensei/sensei_components.py  

clean-doc:  
	rm -rf $(DSTDOC)
