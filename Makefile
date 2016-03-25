NGX_PREFIX = /usr/local/nginx

PREFIX = /usr/local/
LUA_INC_DIR = $(PREFIX)/include
LUA_LIB_DIR = $(PREFIX)/lib/lua/$(LUA_VERSION)
INSTALL = install

.PHONY: all test install

all: ;

install: all
	$(INSTALL) -d $(DESTDIR)/$(LUA_LIB_DIR)/resty
	$(INSTALL) -m 644 lib/resty/*.lua $(DESTDIR)/$(LUA_LIB_DIR)/resty

test: ;
