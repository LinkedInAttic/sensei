package com.senseidb.search.relevance.impl;

import org.json.JSONException;

import com.senseidb.search.req.ErrorType;

public class RelevanceException extends JSONException
{
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private Throwable         myCause;

  /**
   * Gets the cause of this throwable. It is for JDK 1.3 compatibility.
   */
  public Throwable getCause()
  {
    return (myCause == this ? null : myCause);
  }

  /**
   * Initializes the cause of this throwable. It is for JDK 1.3 compatibility.
   */
  public synchronized Throwable initCause(Throwable cause)
  {
    myCause = cause;
    return this;
  }

  private String message;
  private int    errorcode = Integer.MIN_VALUE;

  /**
   * Gets a long message if it is available.
   */
  public String getMessage()
  {
    if (message != null)
      return message;
    else
      return this.toString();
  }
  
  /**
   * Gets the errorcode if it is available.
   */
  public int getErrorCode()
  {
    return this.errorcode;
  }
  

  /**
   * Constructs a RelevanceException with a message.
   * 
   * @param msg
   *          the message.
   */
  public RelevanceException(String msg)
  {
    super(msg);
    errorcode = ErrorType.UnknownError.getDefaultErrorCode();
    message = msg;
    initCause(null);
  }

  /**
   * Constructs a RelevanceException with an error code and message.
   */
  public RelevanceException(ErrorType errorType, String message)
  {
    super(message);
    this.message = message;
    this.errorcode = errorType.getDefaultErrorCode();
    initCause(null);
  }
  
  /**
   * Constructs a RelevanceException with an error code and message and exception.
   */
  public RelevanceException(ErrorType errorType, String message, Exception e)
  {
    super(message);
    this.message = message;
    this.errorcode = errorType.getDefaultErrorCode();
    initCause(e);
  }

  /**
   * Constructs a RelevanceException with an <code>Exception</code> representing the
   * cause.
   * 
   * @param e
   *          the cause.
   */
  public RelevanceException(Throwable e)
  {
    super("by " + e.toString());
    message = null;
    errorcode = ErrorType.UnknownError.getDefaultErrorCode();
    initCause(e);
  }

  /**
   * Constructs a RelevanceException with a detailed message and an <code>Exception</code>
   * representing the cause.
   * 
   * @param msg
   *          the message.
   * @param e
   *          the cause.
   */
  public RelevanceException(String msg, Throwable e)
  {
    this(msg);
    errorcode = ErrorType.UnknownError.getDefaultErrorCode();
    initCause(e);
  }

  /**
   * Constructs a RelevanceException with an <code>Exception</code>.
   */
  public RelevanceException(Exception e)
  {
    this("exception message: " + e.getMessage(), e);
  }

}
