SUBDIRS=libndag-server src wdcapsniffer testclient

AUTOMAKE_OPTIONS=foreign

ACLOCAL_AMFLAGS=-I m4

format:
	find . -type f -name "*.[ch]" -not -path "./common/*" -exec \
		clang-format -style=file -i {} \;
				
.PHONY: format
