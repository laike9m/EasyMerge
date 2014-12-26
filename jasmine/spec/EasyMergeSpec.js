describe("test Javascript functions in EasyMerge", function () {
  it("test insertLink", function () {
    expect(insertLink("abcdefg")).toEqual("abcdefg");
    expect(insertLink("14/12/26 16:17:47 INFO mapred.JobClient: Running job: job_201411181108_16605"))
    .toEqual("14/12/26 16:17:47 INFO mapred.JobClient: Running job: " +
      "<a style='color:red' href='http://10.100.1.35:50030/jobdetails.jsp?jobid=job_201411181108_16605'" +
      ">job_201411181108_16605</a>");
  });
});
