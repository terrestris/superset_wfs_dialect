### Custom patch for owslib.feature.wfs200 to fix some shortcomings like
###
### 1) adding missing support for resultType parameter in getfeature requests.
### 2) fixing wrong parameter name "query" to "filter" for filter parameter in getfeature requests.
### 3) adding missing support for srsName parameter in getfeature requests.
### 4) adding support for outputFormat query parameter in POST getfeature requests
###
### As soon as this is fixed in owslib, this file can be removed and the
### original openURL can be used instead.

from io import BytesIO
import logging
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

from owslib.etree import etree
from owslib.feature.wfs200 import (
  WebFeatureService_2_0_0 as WebFeatureService_2_0_0_owslib,
  OGC_NAMESPACE,
  ServiceException,
)
from owslib.util import build_get_url, nspath

from .custom_postrequest import PostRequest_2_0_0
from .custom_open_url import openURL


LOGGER = logging.getLogger(__name__)


class WebFeatureService_2_0_0(WebFeatureService_2_0_0_owslib):

    def getfeature(
        self,
        result_type=None,
        typename=None,
        filter=None,
        bbox=None,
        featureid=None,
        featureversion=None,
        propertyname=None,
        maxfeatures=None,
        srsname=None,
        storedQueryID=None,
        storedQueryParams=None,
        method="Get",
        outputFormat=None,
        startindex=None,
        sortby=None,
    ):
        """Override getfeature"""
        storedQueryParams = storedQueryParams or {}
        url = data = None
        if typename and type(typename) == type(""):  # noqa: E721
            typename = [typename]
        if method.upper() == "GET":
            (url) = self.getGETGetFeatureRequest(
                result_type,
                typename,
                filter,
                bbox,
                featureid,
                featureversion,
                propertyname,
                maxfeatures,
                srsname,
                storedQueryID,
                storedQueryParams,
                outputFormat,
                "Get",
                startindex,
                sortby,
            )
            LOGGER.debug("GetFeature WFS GET url %s" % url)
        else:
            url, data = self.getPOSTGetFeatureRequest(
                result_type,
                srsname,
                typename,
                filter,
                bbox,
                featureid,
                featureversion,
                propertyname,
                maxfeatures,
                storedQueryID,
                storedQueryParams,
                outputFormat,
                "Post",
                startindex,
                sortby)

        u = openURL(url, data, method, timeout=self.timeout, headers=self.headers, auth=self.auth)

        # check for service exceptions, rewrap, and return
        # We're going to assume that anything with a content-length > 32k
        # is data. We'll check anything smaller.
        if "Content-Length" in u.info():
            length = int(u.info()["Content-Length"])
            have_read = False
        else:
            data = u.read()
            have_read = True
            length = len(data)

        if length < 32000:
            if not have_read:
                data = u.read()

            try:
                tree = etree.fromstring(data)
            except BaseException:
                # Not XML
                return BytesIO(data)
            else:
                if tree.tag == "{%s}ServiceExceptionReport" % OGC_NAMESPACE:
                    se = tree.find(nspath("ServiceException", OGC_NAMESPACE))
                    raise ServiceException(str(se.text).strip())
                else:
                    return BytesIO(data)
        else:
            if have_read:
                return BytesIO(data)
            return u

    def getGETGetFeatureRequest(
        self,
        result_type=None,
        typename=None,
        filter=None,
        bbox=None,
        featureid=None,
        featureversion=None,
        propertyname=None,
        maxfeatures=None,
        srsname=None,
        storedQueryID=None,
        storedQueryParams=None,
        outputFormat=None,
        method="Get",
        startindex=None,
        sortby=None,
    ):
        """Override getGETGetFeatureRequest"""
        storedQueryParams = storedQueryParams or {}

        base_url = next(
            (
                m.get("url")
                for m in self.getOperationByName("GetFeature").methods
                if m.get("type").lower() == method.lower()
            )
        )

        request = {"service": "WFS", "version": self.version, "request": "GetFeature"}

        if result_type:
            request["resultType"] = result_type
        # check featureid
        if featureid:
            request["featureid"] = ",".join(featureid)
        elif bbox:
            request["bbox"] = self.getBBOXKVP(bbox, typename)
        elif filter:
            request["filter"] = str(filter)
        if typename:
            typename = (
                [typename] if isinstance(typename, str) else typename
            )  # noqa: E721
            if int(self.version.split(".")[0]) >= 2:
                request["typenames"] = ",".join(typename)
            else:
                request["typename"] = ",".join(typename)
        if propertyname:
            request["propertyname"] = ",".join(propertyname)
        if sortby:
            request["sortby"] = ",".join(sortby)
        if featureversion:
            request["featureversion"] = str(featureversion)
        if maxfeatures:
            if int(self.version.split(".")[0]) >= 2:
                request["count"] = str(maxfeatures)
            else:
                request["maxfeatures"] = str(maxfeatures)
        if srsname:
            request["srsname"] = str(srsname)

            # Check if desired SRS is supported by the service for each
            # typename. Warning will be thrown if that SRS is not allowed.
            for name in typename:
                _ = self.getSRS(srsname, name)
        if startindex:
            request["startindex"] = str(startindex)
        if storedQueryID:
            request["storedQuery_id"] = str(storedQueryID)
            for param in storedQueryParams:
                request[param] = storedQueryParams[param]
        if outputFormat is not None:
            request["outputFormat"] = outputFormat

        return build_get_url(base_url, request)

    def getPOSTGetFeatureRequest(
        self,
        result_type=None,
        srsname=None,
        typename=None,
        filter=None,
        bbox=None,
        featureid=None,
        featureversion=None,
        propertyname=None,
        maxfeatures=None,
        storedQueryID=None,
        storedQueryParams=None,
        outputFormat=None,
        method="Post",
        startindex=None,
        sortby=None,
    ):
        """Override getPOSTGetFeatureRequest"""

        try:
            base_url = next(
                (
                    m.get("url")
                    for m in self.getOperationByName("GetFeature").methods
                    if m.get("type").lower() == method.lower()
                )
            )
        except StopIteration:
            base_url = self.url

        if not typename and filter:
            return base_url, filter

        request = self.create_post_request()

        if storedQueryID:
            if self.version in ["1.0.0", "1.1.0"]:
                LOGGER.warning("Stored queries are only supported in version 2.0.0 and above.")
                return None

            storedQueryParams = storedQueryParams or {}
            request.create_storedquery(storedQueryID, storedQueryParams)
            data = request.to_string()
            return base_url, data

        typename = (
            [typename] if isinstance(typename, str) else typename
        )  # noqa: E721
        typenames = ",".join(typename)

        request.create_query(typenames)

        nsmap = self.identification._root.nsmap
        request.register_typenames_ns(typenames, nsmap)

        if result_type:
            request.set_resulttype(result_type)
        if srsname:
            request.set_srsname(srsname)

        if featureid:
            featureid = (
                [featureid] if isinstance(featureid, str) else featureid
            )
            request.set_featureid(featureid)
        elif bbox:
            request.set_bbox(self.getBBOXPost(bbox, typename))
        elif filter is not None:
            request.set_filter(filter)

        if featureversion:
            request.set_featureversion(str(featureversion))
        if maxfeatures:
            request.set_maxfeatures(maxfeatures)
        if outputFormat:
            request.set_outputformat(outputFormat)
            # Ensure that outputFormat is also set as query parameter,
            # as some WFS servers require it to be present in the URL even for POST requests
            # TODO check, if this is actually needed, or if it is already solved
            #      by fixing the set_outputformat method in the PostRequest class.
            parts = urlparse(base_url)
            query = dict(parse_qsl(parts.query))
            query["outputFormat"] = outputFormat
            base_url = urlunparse(parts._replace(query=urlencode(query)))

        if propertyname:
            propertyname = (
                [propertyname] if isinstance(propertyname, str) else propertyname
            )
            request.set_propertyname(propertyname)
        if sortby:
            sortby = (
                [sortby] if isinstance(sortby, str) else sortby
            )
            request.set_sortby(sortby)
        if startindex:
            request.set_startindex(startindex)

        data = request.to_string()
        return base_url, data

    def create_post_request(self):
        return PostRequest_2_0_0()
