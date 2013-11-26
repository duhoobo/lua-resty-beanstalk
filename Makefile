NGX_PREFIX = /usr/local/nginx

PREFIX = /usr/local/
LUA_INC_DIR = $(PREFIX)/include
LUA_LIB_DIR = $(PREFIX)/lib/lua/$(LUA_VERSION)

.PHONY: all test install
 
all: ;

install: all
	$(INSTALL) -d $(DESTDIR)/$(LUA_LIB_DIR)/nginx
	$(INSTALL) lib/nginx/*.lua $(DESTDIR)/$(LUA_LIB_DIR)/nginx

test: ;
