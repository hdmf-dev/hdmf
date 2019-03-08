.. _sec_nwbformat_overview:

NWB:N File Format
=================

The `NWB Format <https://nwb-schema.readthedocs.io>`_ is built around two concepts:
*TimeSeries* and *ProcessingModules*.

:ref:`timeseries_overview` are objects for storing time series data, and :ref:`modules_overview` are objects
for storing and grouping analyses. The following sections describe these classes in further detail.


.. _file_overview:

NWBFile
-------

NWB files are represented in HDMF with *NWBFile* objects. :py:class:`~hdmf.file.NWBFile`
objects provide functionality for creating :ref:`timeseries_overview` datasets
and :ref:`modules_overview`, as well as functionality for storing
experimental metadata and other metadata related to data provenance.

.. _timeseries_overview:

TimeSeries
----------

TimeSeries objects store time series data. These Python objects correspond to TimeSeries specifications
provided by the NWB format specification. Like the NWB specification, TimeSeries Python objects
follow an object-oriented inheritance pattern. For example, the class :py:class:`~hdmf.base.TimeSeries`
serves as the base class for all other TimeSeries types.


The following TimeSeries objects are provided by the API and NWB specification:

  * :py:class:`~hdmf.ecephys.ElectricalSeries`

    * :py:class:`~hdmf.ecephys.SpikeEventSeries`

  * :py:class:`~hdmf.misc.AnnotationSeries`
  * :py:class:`~hdmf.misc.AbstractFeatureSeries`
  * :py:class:`~hdmf.image.ImageSeries`

    * :py:class:`~hdmf.image.ImageMaskSeries`
    * :py:class:`~hdmf.image.OpticalSeries`
    * :py:class:`~hdmf.ophys.TwoPhotonSeries`

  * :py:class:`~hdmf.image.IndexSeries`
  * :py:class:`~hdmf.misc.IntervalSeries`
  * :py:class:`~hdmf.ophys.OptogeneticSeries`
  * :py:class:`~hdmf.icephys.PatchClampSeries`

    * :py:class:`~hdmf.icephys.CurrentClampSeries`

      * :py:class:`~hdmf.icephys.IZeroClampSeries`

    * :py:class:`~hdmf.icephys.CurrentClampStimulusSeries`
    * :py:class:`~hdmf.icephys.VoltageClampSeries`
    * :py:class:`~hdmf.icephys.VoltageClampStimulusSeries`

  * :py:class:`~hdmf.ophys.RoiResponseSeries`
  * :py:class:`~hdmf.behavior.SpatialSeries`


.. _modules_overview:

Processing Modules
------------------

Processing modules are objects that group together common analyses done during processing of data.
Processing module objects are unique collections of analysis results. To standardize the storage of
common analyses, NWB provides the concept of an *NWBDataInterface*, where the output of
common analyses are represented as objects that extend the :py:class:`~hdmf.core.NWBDataInterface` class.
In most cases, you will not need to interact with the :py:class:`~hdmf.core.NWBDataInterface` class directly.
More commonly, you will be creating instances of classes that extend this class.

The following analysis :py:class:`~hdmf.core.NWBDataInterface` objects are provided by the API and NWB specification:

  * :py:class:`~hdmf.behavior.BehavioralEpochs`
  * :py:class:`~hdmf.behavior.BehavioralEvents`
  * :py:class:`~hdmf.behavior.BehavioralTimeSeries`
  * :py:class:`~hdmf.behavior.CompassDirection`
  * :py:class:`~hdmf.ophys.DfOverF`
  * :py:class:`~hdmf.ecephys.EventDetection`
  * :py:class:`~hdmf.ecephys.EventWaveform`
  * :py:class:`~hdmf.behavior.EyeTracking`
  * :py:class:`~hdmf.ecephys.FeatureExtraction`
  * :py:class:`~hdmf.ecephys.FilteredEphys`
  * :py:class:`~hdmf.ophys.Fluorescence`
  * :py:class:`~hdmf.ophys.ImageSegmentation`
  * :py:class:`~hdmf.retinotopy.ImagingRetinotopy`
  * :py:class:`~hdmf.ecephys.LFP`
  * :py:class:`~hdmf.behavior.MotionCorrection`
  * :py:class:`~hdmf.behavior.Position`

Additionally, the :py:class:`~hdmf.base.TimeSeries` described :ref:`above <timeseries_overview>`
are also subclasses of :py:class:`~hdmf.core.NWBDataInterface`, and can therefore be used anywhere
:py:class:`~hdmf.core.NWBDataInterface` is allowed.

.. note::

    In addition to ``NWBContainer`` which functions as a common base type for Group objects
    ``NWBData`` provides a common base for the specification of datasets in the NWB:N format.
