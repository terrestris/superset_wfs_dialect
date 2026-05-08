from owslib.etree import etree
from owslib.feature.postrequest import PostRequest_2_0_0 as PostRequest_2_0_0_owslib

### Custom patch for owslib.feature.postrequest to add support for
### 1) resultType parameter
### 2) namespace registration for typenames
### 3) srsName parameter
### 4) outputFormat query parameter in POST getfeature requests (is wrongly written in lower case in owslib)
### As soon as this is fixed in owslib, this file can be removed and the original PostRequest can be used instead.
class PostRequest_2_0_0(PostRequest_2_0_0_owslib):

    def set_resulttype(self, result_type):
        """Set the result type for the request."""
        self._root.set("resultType", result_type)

    def set_srsname(self, srs_name):
        """Set the srsName for the request."""
        self._query.set("srsName", srs_name)

    def set_outputformat(self, outputFormat):
        """Set the output format.

        Verify the available formats with a GetCapabilites request.
        """
        self._root.set("outputFormat", outputFormat)

    def register_typenames_ns(self, typenames, wfs_nsmap):

        nsmap = self._root.nsmap

        """Split typenames on ',' and register every prefix (':') as ns"""
        for t in typenames.split(","):
            if ":" in t:
                prefix = t.split(":")[0]
                ns_url = wfs_nsmap.get(prefix)
                if ns_url:
                    nsmap[prefix] = ns_url

        new_root = etree.Element(self._root.tag, nsmap=nsmap)
        new_root[:] = self._root[:]
        new_root.attrib.update(self._root.attrib)
        self._root = new_root
