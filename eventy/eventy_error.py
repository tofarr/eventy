class EventyError(Exception):
    pass


class SkipException(EventyError):
    """Exception raised to indicate that an event should be skipped on the current worker.

    When this exception is raised by a subscriber, it indicates that the event processing
    should be skipped for this worker, resulting in an EventResult with success=False
    and a detail message indicating that the event was skipped.
    """
